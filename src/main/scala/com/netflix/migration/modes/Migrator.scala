package com.netflix.migration.modes

import com.netflix.migration.data.{Job, MigrationConfig}
import com.netflix.migration.providers.{MetacatServiceProvider, MigrationConfigProvider, SparkSessionProvider}
import com.netflix.migration.services.{MetacatService, StorageService, StorageServiceImpl}
import com.netflix.migration.utils.MigrationConsts.{HIVE_CSV_TEXT, HIVE_PARQUET, HIVE_SUFFIX, PauseReason}
import com.netflix.migration.utils.MigrationRunnerUtils.getFnName
import com.netflix.migration.utils.{MigrationRunnerUtils, Utils}
import com.netflix.migration.utils.Utils.{consistentHash, isHiveTable, isHiveTableEmpty}
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.SparkSession

case class Migrator() extends CommandMode with StrictLogging {

  val conf: MigrationConfig = MigrationConfigProvider.getMigrationConfig
  val spark: SparkSession = SparkSessionProvider.getSparkSession
  val metacatService: MetacatService = MetacatServiceProvider.getMetacatService
  @transient private[modes] lazy val storageService: StorageService = StorageServiceImpl(conf)

  def getRelevantJobs(): List[Job] = {
    val jobsQuery =
      s"""
         |SELECT *
         |FROM ${conf.queueName}
         |WHERE ((to_be_processed = true
         |AND comm_level1_date IS NOT NULL
         |AND (comm_level1_date < NOW() - INTERVAL initial_gap_days DAY)
         |AND state = 'Ready' AND desired_state = 'WritesBlocked')
         |OR (in_process = true
         |AND state != desired_state))
         |AND migration_paused = false
         |""".stripMargin
    storageService.getJobs(jobsQuery)
  }

  def run(): Unit = {
    var jobs = getRelevantJobs()
    if (jobs.isEmpty) {
      logger.warn(s"Migrator with runid=${conf.runId} did not find any jobs to process")
      return
    }
    if (!conf.batchId.isEmpty) {
      val numJobsBeforeFilter = jobs.size
      jobs = MigrationRunnerUtils.applyBatchIdFilterOnJobs(spark, conf, jobs)
      logger.info(
        s"Found ${jobs.size} matching batchId filter: ${conf.batchId.get} out of $numJobsBeforeFilter")
    }

    logger.info(
      s"Migrator with runid=${conf.runId} found ${jobs.size} jobs to process. Jobs list: $jobs")
    for (job <- jobs) {
      if (consistentHash(job.id, conf.numMigrators) == conf.migratorId) {
        migrateJob(job)
      }
    }
  }

  /**
   * Migrate the job entity.
   *
   * @param job
   *   the job entity to be migrated
   */
  def migrateJob(job: Job): Unit = {
    val fullTableName = Utils.getFullyQualifiedTableName(job)
    val tableDtoOpt = metacatService.getTable(job.catalogName, job.dbName, job.tblName)
    if (tableDtoOpt.isEmpty && !metacatService
        .tableExists(job.catalogName, job.dbName, job.tblName + HIVE_SUFFIX)) {
      job.migrationPaused = 1
      job.pauseReason = PauseReason.TABLE_NOT_FOUND.toString
      storageService.updateJob(job)
      logger.warn(s"Migration paused for table $fullTableName. Pause reason: Table not found.")
      return
    }

    if (job.toBeProcessed == 1) {
      if (isHiveTable(job.catalogName, job.dbName, job.tblName)) {
        if (job.stgFormat == HIVE_PARQUET || isHiveTableEmpty(job)) {
          job.inProcess = 1
          job.toBeProcessed = 0
          storageService.updateJob(job)
          logger.info(s"Started Migrator for table: $fullTableName. Job: $job")
        } else {
          job.migrationPaused = 1
          job.pauseReason = PauseReason.IS_CSV_TEXT.toString
          storageService.updateJob(job)
          logger.info(
            s"Migration paused for table $fullTableName. Table is in $HIVE_CSV_TEXT format.")
          return
        }
      } else {
        job.migrationPaused = 1
        job.pauseReason = PauseReason.IS_ALREADY_ICEBERG.toString
        storageService.updateJob(job)
        logger.info(
          s"The table: $fullTableName is already in Iceberg format. " +
            s"Skipping and pausing migration. Job: $job")
        return
      }
    }
    var fun = Utils.getJobNextAction(job)
    while (!fun.isEmpty) {
      val f = fun.get
      f(job)
      fun = Utils.getJobNextAction(job)
      storageService.updateJob(job)
      logger.info(s"Migrator transitioning table: $fullTableName from " +
        s"state: ${job.state} to state: ${job.desiredState} using function: ${getFnName(f)}. Job: $job")
    }
  }
}
