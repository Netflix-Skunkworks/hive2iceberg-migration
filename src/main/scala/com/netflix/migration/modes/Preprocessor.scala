package com.netflix.migration.modes

import com.netflix.migration.data.{Job, JobState, MigrationConfig}
import com.netflix.migration.providers.{MetacatServiceProvider, MigrationConfigProvider, SparkSessionProvider}
import com.netflix.migration.services.{MetacatService, StorageService, StorageServiceImpl}
import com.netflix.migration.utils.MigrationConsts.{HIVE_CSV_TEXT, PauseReason}
import com.netflix.migration.utils.Utils
import com.netflix.migration.utils.Utils.{consistentHash, getDataCategory, getDownstreamUsers, getTableOwners, getTableStorageFormat, isHiveTable, isHiveTableEmpty, tableHasTimestampColumn}
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.SparkSession

import scala.util.Random.shuffle

case class Preprocessor() extends CommandMode with StrictLogging {

  val conf: MigrationConfig = MigrationConfigProvider.getMigrationConfig
  val spark: SparkSession = SparkSessionProvider.getSparkSession
  @transient private[modes] lazy val storageService: StorageService = StorageServiceImpl(conf)
  val metacatService: MetacatService = MetacatServiceProvider.getMetacatService

  def getRelevantJobs(): List[Job] = {
    val jobsQuery =
      s"""
         |SELECT *
         |FROM ${conf.queueName}
         |WHERE ((to_be_processed = false
         |AND in_process = false
         |AND state = desired_state
         |AND desired_state = 'Undefined')
         |OR (in_process = true
         |AND state = desired_state
         |AND desired_state = 'WritesUnblocked'
         |AND comm_level2_date < NOW() - INTERVAL probation_gap_days DAY)
         |OR (in_process = true
         |AND state = desired_state
         |AND desired_state = 'HiveDropped'))
         |AND migration_paused = false
         |LIMIT ${conf.jobsProcessBatchSize}
         |""".stripMargin
    storageService.getJobs(jobsQuery)
  }

  def run(): Unit = {
    val jobs = getRelevantJobs()
    logger.info(s"Preprocessor with runid=${conf.runId} processing jobs=$jobs")
    for (job <- jobs) {
      if (consistentHash(job.id, conf.numPreprocessors) == conf.preprocessorId) {
        processJob(job)
      }
    }
  }

  /**
   * Process the job entity and update its status.
   *
   * @param job
   *   the job entity to be processed
   */
  def processJob(job: Job): Unit = {
    val fullTableName = Utils.getFullyQualifiedTableName(job)
    println(s"Preprocessing job: $fullTableName")
    (job.toBeProcessed, job.inProcess, job.state, job.desiredState) match {
      case (0, 0, JobState.Undefined, JobState.Undefined) =>
        val tableDtoOpt = metacatService.getTable(job.catalogName, job.dbName, job.tblName)
        if (tableDtoOpt.isEmpty) {
          job.migrationPaused = 1
          job.pauseReason = PauseReason.TABLE_NOT_FOUND.toString
          storageService.updateJob(job)
          logger.info(
            s"Migration paused for table $fullTableName. Pause reason: Table not found.")
          return
        }

        job.stgFormat = getTableStorageFormat(job)
        job.dataCategory = getDataCategory(job).toString
        if (job.tblOwners.isEmpty) {
          job.tblOwners = getTableOwners(job)
        }
        if (job.downstreamUsers.isEmpty) {
          job.downstreamUsers = getDownstreamUsers(job)
        }
        storageService.updateJob(job)

        if (isHiveTable(job.catalogName, job.dbName, job.tblName) || isHiveTableEmpty(job)) {
          job.state = JobState.Ready
          job.desiredState = JobState.WritesBlocked
          job.toBeProcessed = 1
          storageService.updateJob(job)
        } else {
          job.migrationPaused = 1
          job.pauseReason = PauseReason.IS_ALREADY_ICEBERG.toString
          storageService.updateJob(job)
          logger.info(
            s"The table: $fullTableName is already " +
              s"in Iceberg format. Skipping and pausing migration. Job: $job")
        }

      case (_, 1, JobState.WritesUnblocked, JobState.WritesUnblocked) =>
        job.desiredState = JobState.HiveDropped
        storageService.updateJob(job)

      case (_, 1, JobState.HiveDropped, JobState.HiveDropped) =>
        job.inProcess = 0
        storageService.updateJob(job)

      case _ => // do nothing
    }
  }
}
