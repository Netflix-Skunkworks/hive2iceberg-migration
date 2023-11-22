# Hive to Iceberg Migration Tooling

## Migration queue schema

Migration tooling uses `jobs` table as the queue. The table is created using the following SQL command:

```sql
CREATE TABLE `jobs` (
  `task_id` int NOT NULL AUTO_INCREMENT,
  `catalog_name` varchar(30) NOT NULL,
  `db_name` varchar(255) NOT NULL,
  `tbl_name` varchar(255) NOT NULL,
  `stg_format` varchar(30) DEFAULT NULL,
  `data_category` varchar(255) NOT NULL DEFAULT 'UNKNOWN',
  `tbl_owners` json DEFAULT NULL,
  `downstream_users` json DEFAULT NULL,
  `to_be_processed` tinyint(1) NOT NULL DEFAULT '0',
  `in_process` tinyint(1) NOT NULL DEFAULT '0',
  `state` varchar(30) NOT NULL DEFAULT 'Undefined',
  `desired_state` varchar(30) NOT NULL DEFAULT 'Undefined',
  `initial_gap_days` int NOT NULL DEFAULT '14',
  `probation_gap_days` int NOT NULL DEFAULT '0',
  `comm_level1_date` timestamp NULL DEFAULT NULL,
  `comm_level2_date` timestamp NULL DEFAULT NULL,
  `comm_level3_date` timestamp NULL DEFAULT NULL,
  `shadow_watermark` mediumtext,
  `migration_paused` tinyint(1) NOT NULL DEFAULT '0',
  `pause_reason` varchar(512) NOT NULL DEFAULT 'None',
  `run_id` varchar(4096) DEFAULT NULL,
  `shadow_status` varchar(30) DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `last_updated_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `deferred_date` timestamp NULL DEFAULT NULL,
  `reverter_run_id` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`task_id`),
  UNIQUE KEY `uniq_name` (`catalog_name`,`db_name`,`tbl_name`)
) ENGINE=InnoDB AUTO_INCREMENT=452391 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
```

## Modes of operation

Tooling provides five components/(modes of operation): PREPROCESSOR, COMMUNICATOR, MIGRATOR, REVERTER, SHADOWER

Each mode of operation can be a separate scheduled workflow, for an instance, below workflow creates an scheduled instance for PREPROCESSOR:
```
Trigger:
  cron: 0 0 * * * # Means: run everyday at midnight
  tz: US/Pacific # Means: specified cron is in Pacific Time.
Workflow:
  id: hive2iceberg_migration_preprocessor
  name: hive2iceberg_migration_preprocessor
  jobs:
    - job:
        id: hive_to_iceberg_migration_job
        genie:
          tags:
            - ignore_lineage_usage
        spark:
          app_args:
            - mode=PREPROCESSOR
            - jobsProcessBatchSize=5000
            - dbEnv="prod"
            - local=false
            - dryrun=false
          class: ${migration_main}
          script: $S3{${migration_jar}}
          version: ${migration_spark_version}
        type: Spark     
