package com.netflix.migration.modes

import com.netflix.bdp.IncrementalRefreshShadowTool
import com.netflix.migration.data.{Job, MigrationConfig}
import com.netflix.migration.providers.{MigrationConfigProvider, SparkSessionProvider}
import com.netflix.migration.services.{StorageService, StorageServiceImpl}
import com.netflix.migration.utils.MigrationConsts.HIVE_SUFFIX
import com.netflix.migration.utils.Utils
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.SparkSession

case class Shadower() extends CommandMode with StrictLogging {

  val conf: MigrationConfig = MigrationConfigProvider.getMigrationConfig
  val spark: SparkSession = SparkSessionProvider.getSparkSession
//  val sparkSessionPool: Array[SparkSession] = SparkSessionPoolProvider.getSparkSessionPool
  @transient private[modes] lazy val storageService: StorageService = StorageServiceImpl(conf)

  def getRelevantJobs(): List[Job] = {
    val jobsQuery =
      s"""
         |SELECT *
         |FROM ${conf.queueName}
         |WHERE state = 'WritesUnblocked'
         |AND desired_state = 'WritesUnblocked'
         |AND probation_gap_days > 0
         |AND shadow_status is NULL
         |AND migration_paused = false
         |""".stripMargin
    storageService.getJobs(jobsQuery)
  }

  def run(): Unit = {
    val jobs = getRelevantJobs()
    logger.info(s"Shadower with runid=${conf.runId} processing jobs=$jobs")
    for (job <- jobs) {
      if (!Utils.hiveIcebergTablesInSync(job)) {
        shadowJob(job)
      }
    }
  }

  /**
   * Shadow the job entity.
   *
   * @param job
   *   the job entity to be shadowed
   */
  def shadowJob(job: Job): Unit = {
    val tableName = Utils.getFullyQualifiedTableName(job)
    val destinationTableName = s"$tableName$HIVE_SUFFIX"
    job.shadowStatus = "Shadowing"
    logger.info(s"Running shadower for $tableName to destination $destinationTableName")
    storageService.updateJob(job)
    copyDataSrcToDst(tableName, destinationTableName)
    job.shadowStatus = null
    storageService.updateJob(job)
  }

  /**
   * Copy data from source table to destination table.
   *
   * @param srcTable
   *   The name of the source table.
   * @param dstTable
   *   The name of the destination table.
   */
  def copyDataSrcToDst(srcTable: String, dstTable: String): Unit = {
    val shadowTool =
      new IncrementalRefreshShadowTool(srcTable, dstTable, spark, 50, 3, 50)
    shadowTool.mirrorUpdates()
  }
}
