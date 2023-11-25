package com.netflix.migration

import com.netflix.migration.data.MigrationCommandMode._
import com.netflix.migration.data.{MigrationCommandMode, MigrationConfig}
import com.netflix.migration.modes._
import com.netflix.migration.providers.{IcebergTableServiceProvider, MetacatServiceProvider, MigrationConfigProvider, NdcServiceProvider, SparkSessionProvider}
import com.netflix.migration.services.{MetacatService, MetacatServiceImpl, NdcService, NdcServiceImpl, TrinoIcebergTableServiceImpl}
import com.netflix.migration.utils.{MigrationConsts, StringUtils, Utils}
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.SparkSession

import java.time.{Instant, LocalDate}

case class MigrationRunner() extends StrictLogging {

  val conf: MigrationConfig = MigrationConfigProvider.getMigrationConfig
  val spark: SparkSession = SparkSessionProvider.getSparkSession

  def process(): Unit = {
    logger.info(
      s"Started ${conf.commandMode} with date=${conf.processDate} and runid=${conf.runId}")
    try {
      conf.commandMode match {
        case COMMUNICATOR => Communicator().run()
        case PREPROCESSOR => Preprocessor().run()
        case MIGRATOR => Migrator().run()
        case REVERTER => Reverter().run()
        case SHADOWER => Shadower().run()
      }
    } finally {
      logger.info(
        s"Finished ${conf.commandMode} with date=${conf.processDate} and runid=${conf.runId}")
    }
  }
}

object MigrationRunner extends StrictLogging {
  var tablesToBeReverted: String = _
  def main(args: Array[String]): Unit = {
    logger.info("Migration app. Instantiating Spark Session")
    val argsMap = StringUtils.parseArgs(args)
    logger.info("Args received: " + argsMap)
    createMigrationConf(argsMap)
    val spark: SparkSession = SparkSessionProvider.getSparkSession
    try {
      if (Utils.migrationConf.dryRun) {
        // TODO: Extend the dryRun mode (Each command mode should have a dryRun implementation)
        logger.info("Spark version running: " + spark.version)
      } else {
        tablesToBeReverted = argsMap.getOrElse("tablesToBeReverted", null)
        val runner = MigrationRunner()
        runner.process()
      }
    } finally {
      logger.info("Stopping spark session..")
      spark.stop()
    }
  }

  /**
   * Create a MigrationConfig object from command line arguments
   * @param argsMap
   *   A map of command line arguments, where the key is the argument name and the value is the
   *   argument value
   * @return
   *   A MigrationConfig object
   */
  def createMigrationConf(argsMap: Map[String, String]): MigrationConfig = {
    val migrationConf = MigrationConfig(
      commandMode = MigrationCommandMode.parse(argsMap.getOrElse("mode", "MIGRATOR")),
      runLocally = argsMap.getOrElse("local", "true").toUpperCase.equals("TRUE"),
      dryRun = argsMap
        .getOrElse("dryrun", "false")
        .toUpperCase
        .equals("TRUE"), // default to dryRun mode for now
      processDate = LocalDate.now(),
      processStartTime = Instant.now(),
      runId = sys.env.getOrElse("GENIE_JOB_ID", java.util.UUID.randomUUID.toString),
      distributedMode = argsMap.getOrElse("distributed", "false").toUpperCase.equals("TRUE"),
      dbEnv = argsMap
        .getOrElse("dbEnv", MigrationConsts.TEST_ENV)
        .toLowerCase, // Use test database by default
      jobsProcessBatchSize = argsMap
        .getOrElse("jobsProcessBatchSize", MigrationConsts.jobsProcessBatchSize.toString)
        .toInt,
      queueName = argsMap.getOrElse("queueName", MigrationConsts.queueName),
      archiveHive = argsMap.getOrElse("archiveHive", "true").toUpperCase.equals("TRUE"),
      setIcebergAcls = argsMap.getOrElse("setIcebergAcls", "true").toUpperCase.equals("TRUE"),
      dbName = argsMap.getOrElse("dbName", MigrationConsts.JOBS_DB_NAME),
      batchId = argsMap.get("batchid"),
      batchTableName = argsMap.getOrElse("batchTableName", MigrationConsts.batchTableName),
      migratorId = argsMap.getOrElse("migratorId", MigrationConsts.MIGRATOR_ID.toString).toInt,
      numMigrators =
        argsMap.getOrElse("numMigrators", MigrationConsts.NUM_MIGRATORS.toString).toInt,
      preprocessorId =
        argsMap.getOrElse("preprocessorId", MigrationConsts.PREPROCESSOR_ID.toString).toInt,
      numPreprocessors =
        argsMap.getOrElse("numPreprocessors", MigrationConsts.NUM_PREPROCESSORS.toString).toInt)

    MigrationConfigProvider.init(migrationConf)
    val metacatService: MetacatService = MetacatServiceImpl(migrationConf)
    MetacatServiceProvider.init(metacatService)
    val ndcService: NdcService = NdcServiceImpl(migrationConf)
    NdcServiceProvider.init(ndcService)
    IcebergTableServiceProvider.init(TrinoIcebergTableServiceImpl(migrationConf))
    if (SparkSessionProvider.getSparkSession == null) {
      lazy val spark: SparkSession = Utils.getSparkSession()
      SparkSessionProvider.init(spark)
    }
    migrationConf
  }
}
