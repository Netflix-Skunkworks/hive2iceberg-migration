# Hive to Iceberg Migration Tooling

Note: This repository is intended to be reference and will not be actively maintained by Netflix.

## Design

### Components and Responsibilities:

To automate the migration from Hive to Iceberg, we can use several key components. The "Metadata Table" (MT) acts as both a job queue and metadata store for tables to be migrated. 
For the newly inserted jobs in the MT, the “Preprocessor” (PR)  populates the information such as table owners, downstream users, current state, and desired state etc and makes it available for other components to process the jobs. The "Communicator" (CR) scans the MT for jobs and retrieves the list of table owners and downstream users from the MT for these jobs. Based on the stage of migration recorded in the MT, the CR sends the appropriate level of communication (level 1, level 2, or level 3) and stores the communication level and timestamp in the MT. The "Migrator" (MR) looks at the MT for jobs to be processed and advances to the next state of migration based on the communication level. The MR also stores information about the current state of migration in the MT. These components can be scheduled as workflow jobs that run on a regular basis. The "Shadower" (SR) selects tables in the probation period and performs shadowing from the new Iceberg table to the original Hive table with the _hive suffix. The "Reverter" (RT) can be used to revert the Iceberg table to the original Hive table and pause the migration during the probation period.

### Metadata Table / Job Queue

Migration tooling uses `jobs` table as the queue. The table is created using the following SQL command:

```sql
CREATE TABLE `jobs` (
  `task_id` int NOT NULL AUTO_INCREMENT, -- Unique identifier for each task
  `catalog_name` varchar(30) NOT NULL, -- Name of the catalog
  `db_name` varchar(255) NOT NULL, -- Name of the database
  `tbl_name` varchar(255) NOT NULL, -- Name of the table
  `stg_format` varchar(30) DEFAULT NULL, -- Storage format of the table
  `data_category` varchar(255) NOT NULL DEFAULT 'UNKNOWN', -- Category of the data
  `tbl_owners` json DEFAULT NULL, -- JSON array of table owners
  `downstream_users` json DEFAULT NULL, -- JSON array of downstream users
  `to_be_processed` tinyint(1) NOT NULL DEFAULT '0', -- Flag indicating if the job is ready to be processed
  `in_process` tinyint(1) NOT NULL DEFAULT '0', -- Flag indicating if the job is currently being processed
  `state` varchar(30) NOT NULL DEFAULT 'Undefined', -- Current state of the job
  `desired_state` varchar(30) NOT NULL DEFAULT 'Undefined', -- Desired state of the job
  `initial_gap_days` int NOT NULL DEFAULT '14', -- Initial gap days before processing the job
  `probation_gap_days` int NOT NULL DEFAULT '0', -- Probation gap days before processing the job
  `comm_level1_date` timestamp NULL DEFAULT NULL, -- Timestamp of level 1 communication
  `comm_level2_date` timestamp NULL DEFAULT NULL, -- Timestamp of level 2 communication
  `comm_level3_date` timestamp NULL DEFAULT NULL, -- Timestamp of level 3 communication
  `shadow_watermark` mediumtext, -- Watermark for shadowing process
  `migration_paused` tinyint(1) NOT NULL DEFAULT '0', -- Flag indicating if the migration is paused
  `pause_reason` varchar(512) NOT NULL DEFAULT 'None', -- Reason for pausing the migration
  `shadow_status` varchar(30) DEFAULT NULL, -- Status of the shadowing process
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, -- Timestamp when the job was created
  `last_updated_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, -- Timestamp when the job was last updated
  PRIMARY KEY (`task_id`), -- Primary key of the table
  UNIQUE KEY `uniq_name` (`catalog_name`,`db_name`,`tbl_name`) -- Unique key constraint on catalog_name, db_name, and tbl_name
) ENGINE=InnoDB AUTO_INCREMENT=452391 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; -- Table engine and character set details
```

### Preprocessor

Preprocessor is a process that selects a job with to_be_processed set to 0 and in_process set to 0, extracts the table owner and downstream users, and assigns them to the job. Also, sets state to Ready and desired_state to WritesBlocked, and sets the to_be_processed flag to 1. 

### Migrator

If it is determined that two weeks have passed since level 1 communication was sent for a specific job/table, Migrator will mark the in_process flag as 1 and reset the to_be_processed flag to 0. The migration of that table then begins until the desired_state and state are both set to WritesUnblocked, which starts the probation period marked by Communicator sending level 2 communication. After two weeks have passed since level 2 communication was sent, Migrator completes the migration by dropping the original Hive table with the _hive suffix. At this point, both the state and desired_state become equal to the HiveDropped state and reset the in_process flag to 0.
Communicator

If the to_be_processed flag is set to 1, the Communicator workflow will send Level 1 communication to notify users that the table will be migrated in 1-2 weeks. This sets the date for comm_level1_date. After 1-2 weeks, the migration tool will select the table for migration and set the in_process flag to 1. The migration of the table will then begin. Once the table is migrated, if the desired_state and state are both set to WritesUnblocked, the Communicator will send Level 2 communication to notify users that the probation period has begun. This sets the date for comm_level2_date. After two weeks, the migration tool will delete the original Hive table. If the state and desired_state are both set to HiveDropped at this point, the Communicator will send Level 3 communication to notify users that the migration is complete and set the comm_level3_date.

### Shadower

If the Hive table watermark does not match the current snapshot_id of the Iceberg table, and if the desired_state and state are both set to WritesUnblocked and shadow_status is set to NULL, then the Shadower will set shadow_status to NOT NULL (some meaningful value) and invoke the Shadow Tool (ST) to incrementally copy the data from the new Iceberg table to the Hive table with the _hive suffix. Once the current ST incremental copy is successful, the most recent snapshot_id that the Hive and Iceberg tables are in sync with is set as the watermark in the Hive TBLPROPERTIES, and the ST sets shadow_status to NULL in a single transaction. 

### Reverter

Reverter is a feature that allows users or migration administrators to revert the primary table to Hive if they encounter issues with the newly created Iceberg table during the probation period. Reverter will not allow user requests if the table migration is in-process (where in_process is set to 1) and it is not in the probation period. When Reverter is activated during the probation period, writes to the Iceberg table are first blocked. Then, the shadow tool is used to update the Hive table (with the _hive suffix) with the newly written data in the Iceberg table. The Iceberg table name is then appended with the _iceberg suffix, while the _hive suffix is removed from the Hive table, making it the primary table. Finally, writes to the primary table are unblocked, while the Iceberg table with the _iceberg suffix is deleted and the migration_paused field is set to true.


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
        spark:
          app_args:
            - mode=PREPROCESSOR
            - jobsProcessBatchSize=5000
            - dbEnv="prod"
            - local=false
            - dryrun=false
          class: ${migration_main}
          script: ${migration_jar}
          version: ${migration_spark_version}
        type: Spark     
