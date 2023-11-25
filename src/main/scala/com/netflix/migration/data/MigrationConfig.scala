package com.netflix.migration.data

import com.netflix.migration.data.MigrationCommandMode.MigrationCommandMode
import com.netflix.migration.utils.MigrationConsts

import java.time.{Instant, LocalDate}

/**
 * Migration tool configuration for a given run.
 *
 * @param commandMode
 *   The command mode to run in: MIGRATOR, PREPROCESSOR, REVERTER etc.
 * @param runLocally
 *   Whether the tool should be run locally in Spark.
 * @param dryRun
 *   Whether this is a dry run.
 * @param processDate
 *   The processing date.
 * @param processStartTime
 *   The processing start time.
 * @param distributedMode
 *   Whether the jobs should be processed on executors in a distributed manner.
 * @param runId
 *   The genie job id, or UUID if run locally.
 * @param dbEnv
 *   The database environment, prod or test.
 */
case class MigrationConfig(
    commandMode: MigrationCommandMode,
    runLocally: Boolean,
    dryRun: Boolean,
    processDate: LocalDate,
    processStartTime: Instant,
    distributedMode: Boolean,
    runId: String,
    dbEnv: String,
    jobsProcessBatchSize: Int,
    queueName: String,
    archiveHive: Boolean,
    setIcebergAcls: Boolean,
    dbName: String,
    batchId: Option[String],
    batchTableName: String,
    migratorId: Int,
    numMigrators: Int,
    preprocessorId: Int,
    numPreprocessors: Int) {
  require(
    dbEnv.equals(MigrationConsts.TEST_ENV) || dbEnv.equals(MigrationConsts.PROD_ENV),
    "Database environment should be one of 'prod' or 'test'")
}
