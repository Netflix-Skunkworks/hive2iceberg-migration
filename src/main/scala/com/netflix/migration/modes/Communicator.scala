package com.netflix.migration.modes

import com.netflix.migration.data.{EmailStrings, Job, JobState, MigrationConfig}
import com.netflix.migration.providers.{MetacatServiceProvider, MigrationConfigProvider, SparkSessionProvider}
import com.netflix.migration.services.{MetacatService, StorageService, StorageServiceImpl}
import com.netflix.migration.utils.{EmailSender, Utils}
import com.netflix.migration.utils.MigrationConsts.{HIVE_CSV_TEXT, HIVE_PARQUET, PauseReason, RECIPIENT_BATCH_SIZE, millisecondsPerDay}
import com.netflix.migration.utils.Utils.{createBooleanMap, isHiveTable, isHiveTableEmpty, jobsGroupByOwners}
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.SparkSession

import java.time.Instant
import scala.util.control.Breaks.break

case class Communicator() extends CommandMode with StrictLogging {
  val conf: MigrationConfig = MigrationConfigProvider.getMigrationConfig
  val metacatService: MetacatService = MetacatServiceProvider.getMetacatService

  val spark: SparkSession = SparkSessionProvider.getSparkSession
  @transient private[modes] lazy val storageService: StorageService = StorageServiceImpl(conf)

  def getRelevantJobs(): List[Job] = {
    val jobsQuery =
      s"""
         |SELECT *
         |FROM ${conf.queueName}
         |WHERE ((to_be_processed = 1
         |AND comm_level1_date IS NULL)
         |OR (in_process = 1
         |AND comm_level1_date IS NOT NULL
         |AND state = 'WritesUnblocked'
         |AND desired_state = 'WritesUnblocked'
         |AND comm_level2_date IS NULL)
         |OR (in_process = 0
         |AND comm_level2_date IS NOT NULL
         |AND state = 'HiveDropped'
         |AND desired_state = 'HiveDropped'
         |AND comm_level3_date IS NULL))
         |AND migration_paused = false
         |""".stripMargin
    storageService.getJobs(jobsQuery)
  }

  def run(): Unit = {
    val jobs = getRelevantJobs()
    logger.info(s"Communicator with runid=${conf.runId} processing jobs=$jobs")
    communicate(jobs)
  }

  /**
   * Send email communication to the job's downstream users and owners.
   *
   * @param jobs
   *   the list of job entities for which the communication needs to be sent
   */
  private def communicate(jobs: List[Job]): Unit = {

    val ownerToTablesMap = jobsGroupByOwners(jobs)
    val ownerToCommunicatedMap = createBooleanMap(ownerToTablesMap)

    for (job <- jobs) {
      val table = s"${job.catalogName}.${job.dbName}.${job.tblName}"
      val currentTimeMillis = Instant.now().toEpochMilli
      val initialDaysInMillis: Long = job.initialGapInDays * millisecondsPerDay
      val probationDaysInMillis: Long = job.probationGapInDays * millisecondsPerDay
      val tblOwners = job.tblOwners.filter(email => email.contains("@netflix.com"))
      var downstreamUsers = job.downstreamUsers.diff(job.tblOwners)
      downstreamUsers = downstreamUsers - "bdw_data_detection@netflix.com"
      downstreamUsers = downstreamUsers.filter(email => email.contains("@netflix.com"))

      def sendEmail(to: Set[String], subject: String, body: String): Unit = {
        EmailSender.send(
          to,
          subject.format(table),
          body.format(table, table, tblOwners.mkString(", ")),
          EmailSender.Text)
      }

      def sendEmailToOwners(subject: String, body: String): Unit = {
        for (owner <- tblOwners) {
          if (!ownerToCommunicatedMap(owner) && owner.contains("@netflix.com")) {
            val tables = ownerToTablesMap(owner)
            EmailSender.send(
              Set(owner),
              subject,
              body.format("\n" + tables.map(table => s"* $table. ").mkString("\n") + "\n"),
              EmailSender.Html)
            ownerToCommunicatedMap.put(owner, true)
          }
        }
      }

      (job.toBeProcessed, job.inProcess, job.state, job.desiredState) match {
        case (1, _, _, _) if job.commLevel1Date < 0 =>
          val fullTableName = Utils.getFullyQualifiedTableName(job)
          if (metacatService.tableExists(job.catalogName, job.dbName, job.tblName)) {
            if (isHiveTable(job.catalogName, job.dbName, job.tblName)) {
              if (job.stgFormat == HIVE_PARQUET || isHiveTableEmpty(job)) {
                if (tblOwners.nonEmpty) {
                  if (job.initialGapInDays > 0) {
                    if (downstreamUsers.nonEmpty) {
                      val (subjectForDownstreamUser, bodyForDownstreamUser) =
                        EmailStrings(
                          job.initialGapInDays,
                          job.probationGapInDays).level1DownstreamUserMsg
                      val downstreamUsersSeq = downstreamUsers.grouped(RECIPIENT_BATCH_SIZE)
                      downstreamUsersSeq.foreach { downstreamUsersBatch =>
                        sendEmail(downstreamUsersBatch, subjectForDownstreamUser, bodyForDownstreamUser)
                      }
                    }

                    val (subjectForOwner, bodyForOwner) =
                      EmailStrings(
                        job.initialGapInDays,
                        job.probationGapInDays).level1TableOwnerMsg
                    sendEmailToOwners(subjectForOwner, bodyForOwner)
                  }
                }
                job.commLevel1Date = currentTimeMillis
                storageService.updateJob(job)
              } else {
                job.migrationPaused = 1
                job.pauseReason = PauseReason.IS_CSV_TEXT.toString
                storageService.updateJob(job)
                logger.info(
                  s"Migration paused for table $fullTableName. Pause reason: Table is in $HIVE_CSV_TEXT format.")
              }
            } else {
              job.migrationPaused = 1
              job.pauseReason = PauseReason.IS_ALREADY_ICEBERG.toString
              storageService.updateJob(job)
              logger.info(
                s"The table: $fullTableName is already " +
                  s"in Iceberg format. Skipping and pausing migration. Job: $job")
            }
          } else {
            job.migrationPaused = 1
            job.pauseReason = PauseReason.TABLE_NOT_FOUND.toString
            storageService.updateJob(job)
            logger.info(
              s"Migration paused for table $fullTableName. Pause reason: Table not found.")
          }

        case (_, 1, JobState.WritesUnblocked, JobState.WritesUnblocked)
            if job.commLevel2Date < 0 && (currentTimeMillis - job.commLevel1Date) >= initialDaysInMillis =>
          if (job.probationGapInDays > 0) {
            val (subject, body) =
              EmailStrings(job.initialGapInDays, job.probationGapInDays).level2
            val to = downstreamUsers.union(tblOwners)
            val toUsers = to.grouped(RECIPIENT_BATCH_SIZE)
            toUsers.foreach { toUsersBatch =>
              sendEmail(toUsersBatch, subject, body)
            }
          }
          job.commLevel2Date = currentTimeMillis
          storageService.updateJob(job)

        case (_, 0, JobState.HiveDropped, JobState.HiveDropped)
            if job.commLevel3Date < 0 && (currentTimeMillis - job.commLevel2Date) >= probationDaysInMillis =>
          if (job.initialGapInDays > 0 || job.probationGapInDays > 0) {
            val (subject, body) =
              EmailStrings(job.initialGapInDays, job.probationGapInDays).level3
            val to = downstreamUsers.union(tblOwners)
            val toUsers = to.grouped(RECIPIENT_BATCH_SIZE)
            toUsers.foreach { toUsersBatch =>
              sendEmail(toUsersBatch, subject, body)
            }
          }
          job.commLevel3Date = currentTimeMillis
          storageService.updateJob(job)

        case _ => // do nothing
      }
    }
  }
}
