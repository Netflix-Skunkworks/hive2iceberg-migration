package com.netflix.migration.utils

/**
 * Migration tool constants.
 */
object MigrationConsts {

  /**
   * URL for the test jobs db.
   */
  final val JOBS_DB_TEST_URL: String = "your-test-rds-db-hostname.region.rds.amazonaws.com"
  final val JOBS_DB_PROD_URL: String =
    "your-prod-rds-db-hostname.region.rds.amazonaws.com"
  final val JOBS_DB_NAME: String = "migrate_01"
  final val HIVE_SUFFIX = "_hive"
  final val ICEBERG_SUFFIX = "_iceberg"
  final val REVERT_SUFFIX = "_revert"
  final val ARCHIVED_SUFFIX = "_archived"
  final val millisecondsPerDay = 86400000L
  final val ICEBERG_MIGRATION_DO_NOT_MODIFY_TAG: String = "iceberg_migration_do_not_modify"
  final val MIGRATED_FROM_HIVE_TAG: String = "h2i_migrated_from_hive"
  final val DO_NOT_RENAME_TAG: String = "do_not_rename"
  final val DO_NOT_DROP_TAG: String = "do_not_drop"
  final val HIVE_ARCHIVED_TAG: String = "h2i_archive"
  final val HIVE_PARQUET: String = "parquet"
  final val HIVE_CSV_TEXT: String = "csv/text"
  final val PROD_ENV = "prod"
  final val TEST_ENV = "test"
  final val TEST_CATALOG = "testhive"
  final val TEST_DATABASE = "transport_temp"
  final val TEST_TABLE_PREFIX = "table_"
  final val DEFAULT_TABLE_OWNER = "akayyoor"
  final val TEST_THREAD_POOL_SIZE = 10
  final val NUM_RETRIES = 3
  final val HIVE_TIMESTAMP_COLUMN_NAME = "timestamp"
  final val METACAT_HOST_URL_PROPERTY = "http://metacat-hosturl"
  final val METACAT_USERNAME_PROPERTY = "hiveToIcebergMigrator"
  final val METACAT_CLIENT_APP_NAME_PROPERTY = METACAT_USERNAME_PROPERTY
  final val METACAT_DATA_MIGRATED_PROPERTY = "migrated_data_location"
  final val NDC_HOST_URL_PROPERTY = "https://data-catalog-host-url"
  final val NDC_USERNAME_PROPERTY = METACAT_USERNAME_PROPERTY
  final val NDC_CLIENT_APP_NAME_PROPERTY = METACAT_CLIENT_APP_NAME_PROPERTY
  final val MIGRATOR_ID = 1
  final val NUM_MIGRATORS = 1
  final val PREPROCESSOR_ID = 1
  final val NUM_PREPROCESSORS = 1
  final val RECIPIENT_BATCH_SIZE = 50
  final val MYSQL_USER = 'user'
  final val MYSQL_PASS = 'pass'
  final val TRINO_JDBC_URL = "jdbc:trino://trino.master.net:7004/iceberg/default"
  final val AWS_SES_SOURCE_ARN = "arn:aws:ses:region:etcetc"
  final val COMMUNICATION_EMAIL_ADDRESS = "h2i-migration-noreply@some-domain.com"

  final val jobsProcessBatchSize = 10
  final val queueName = "jobs"
  final val batchTableName = "prod.bdp.h2i_batches"

  object TableFormat extends Enumeration {
    type TableFormat = Value
    val ICEBERG, HIVE_PARQUET = Value
  }

  object PauseReason extends Enumeration {
    type PauseReason = Value
    val IS_CSV_TEXT, IS_ALREADY_ICEBERG, IS_WAP_TABLE, IS_PSYCHO_TABLE, TABLE_NOT_FOUND,
        MISSING_OWNER, HAS_TIMESTAMP_COLUMN, EMPTY_TABLE, REVERTED =
      Value
  }

  object DataCategory extends Enumeration {
    type DataCategory = Value
    val PI, CORE_PI, NO_PI, UNKNOWN = Value
  }
}
