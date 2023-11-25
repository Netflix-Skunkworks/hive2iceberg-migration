package com.netflix.migration.modes

import com.netflix.migration.MigrationRunner
import com.netflix.migration.data.{Job, JobState, MigrationConfig}
import com.netflix.migration.providers.{MetacatServiceProvider, MigrationConfigProvider}
import com.netflix.migration.services.{MetacatService, StorageService, StorageServiceImpl}
import com.netflix.migration.utils.MigrationConsts.PauseReason
import com.netflix.migration.utils.Utils
import com.netflix.migration.utils.Utils.{isHiveTable, revertFinalizedTable, revertHelper}
import com.typesafe.scalalogging.StrictLogging

case class Reverter() extends CommandMode with StrictLogging {

  val conf: MigrationConfig = MigrationConfigProvider.getMigrationConfig
  val metacatService: MetacatService = MetacatServiceProvider.getMetacatService
  @transient private[modes] lazy val storageService: StorageService = StorageServiceImpl(conf)

  def run(): Unit = {
    val tablesToBeReverted = MigrationRunner.tablesToBeReverted.split(",")
    for (table <- tablesToBeReverted) {
      val tableToBeReverted = table.trim.toLowerCase
      if (tableToBeReverted == null) {
        throw new IllegalArgumentException("Table to be reverted is null")
      }
      val tableParts = tableToBeReverted.split("[.]")
      if (tableParts.length != 3) {
        throw new IllegalArgumentException(
          "Table name is not fully qualified. " +
            "Table name format must be <catalog_name>.<database_name>.<table_name>")
      }
      val jobsQuery =
        s"""
           |SELECT *
           |FROM ${conf.queueName}
           |WHERE catalog_name = '${tableParts(0)}'
           |AND db_name = '${tableParts(1)}'
           |AND tbl_name = '${tableParts(2)}'
           |""".stripMargin
      val jobs = storageService.getJobs(jobsQuery)
      logger.info(s"Reverter with runid=${conf.runId} processing jobs=$jobs")
      if (jobs.size > 1) {
        throw new IllegalArgumentException(
          "There is more than single table to be reverted. " +
            "Please specify single table to be reverted")
      }
      val job = jobs.head
      if (!isHiveTable(job.catalogName, job.dbName, job.tblName) && metacatService.tableExists(
          job.catalogName,
          job.dbName,
          job.tblName)) {
        revertHelper(job)
        revertJob(job)
      }
    }
  }

  /**
   * Revert the state of a job entity.
   *
   * @param job
   *   the job entity to be reverted
   */
  def revertJob(job: Job): Unit = {
    val fullTableName = Utils.getFullyQualifiedTableName(job)
    job.migrationPaused = 1
    job.pauseReason = PauseReason.REVERTED.toString
    job.state = JobState.WritesUnblocked
    job.desiredState = JobState.WritesBlocked
    storageService.updateJob(job)
    var fun = Utils.getJobNextAction(job)
    logger.info(s"Revert started for table: $fullTableName at state: ${job.state}. Job: $job")
    while (!fun.isEmpty) {
      val f = fun.get
      f(job)
      fun = Utils.getJobNextAction(job)
      storageService.updateJob(job)
      logger.info(
        s"Reverter transitioning table: $fullTableName from " +
          s"state: ${job.state} to state: ${job.desiredState}. Job: $job")
    }
  }

  /**
   * Bulk revert set of migration tables to Hive table format.
   *
   * @param tables
   *   The set of tables to be reverted
   */
  def bulkRevertTables(tables: Set[(String, String, String)]): Unit = {
    for (table <- tables) {
      val (catalogName, dbName, tableName) = table
      val job = storageService.getJob(catalogName, dbName, tableName)
      revertHelper(job)
      revertJob(job)
    }
  }

  /**
   * Bulk revert set of migration finalized tables to Hive table format.
   *
   * @param tables
   *   The set of tables to be reverted
   */
  def bulkRevertFinalizedTables(tables: Set[(String, String, String)]): Unit = {
    for (table <- tables) {
      if (revertFinalizedTable(table)) {
        val (catalogName, dbName, tableName) = table
        val job = storageService.getJob(catalogName, dbName, tableName)
        job.toBeProcessed = 1
        job.inProcess = 0
        job.state = JobState.Ready
        job.desiredState = JobState.WritesBlocked
        job.migrationPaused = 1
        job.pauseReason = PauseReason.REVERTED.toString
        storageService.updateJob(job)
      }
    }
  }
}
