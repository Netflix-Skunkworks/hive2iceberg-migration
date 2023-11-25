package com.netflix.migration.utils

import com.netflix.migration.MigrationRunner
import com.netflix.migration.modes.Reverter

import java.io.File
import scala.io.Source

object BulkRevertJobs extends App {

  // Set the path to the file containing the list of table names
  val tableFile = new File(
    "./revert_jobs.txt").getAbsoluteFile

  // Set conf to prod env and get migration conf
  val migrationConf = MigrationRunner.createMigrationConf(Map("dbEnv" -> "prod"))

  // Read the table file and generate a (catalogName, dbName, tableName) triplet for each table
  val tablesSet = Source
    .fromFile(tableFile)
    .getLines()
    .map { table =>
      val Array(catalogName, dbName, tableName) = table.split("/")
      (catalogName, dbName, tableName)
    }
    .toSet

  // Instantiate Reverter object to bulk revert tables
  val reverter = Reverter()
  reverter.bulkRevertFinalizedTables(tablesSet)
}
