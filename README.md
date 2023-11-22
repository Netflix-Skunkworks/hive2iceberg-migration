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
