package com.netflix.migration.utils

import com.netflix.migration.MigrationRunner
import com.netflix.migration.services.StorageServiceImpl

import java.io.File
import scala.io.Source

object IngestJobs extends App {

  // Set the path to the file containing the list of table names
  val tableFile = new File(
    "./jobs.txt").getAbsoluteFile

  // Set conf to prod env and get migration conf
  val migrationConf = MigrationRunner.createMigrationConf(Map("dbEnv" -> "prod"))

  // Connect to the MySQL database and retrieve the existing table names from the jobs table
  val connection = StorageServiceImpl.makeDbConnection(migrationConf)
  val statement = connection.createStatement()
  val resultSet = statement.executeQuery("SELECT catalog_name, db_name, tbl_name FROM jobs")
  val existingTableNames = collection.mutable.Set[(String, String, String)]()
  while (resultSet.next()) {
    existingTableNames.add(
      (
        resultSet.getString("catalog_name"),
        resultSet.getString("db_name"),
        resultSet.getString("tbl_name")))
  }
  resultSet.close()
  statement.close()

  // Read the table file and generate a VALUES clause for each table that is not already in the jobs table
  val valuesSet = Source
    .fromFile(tableFile)
    .getLines()
    .map { table =>
      val Array(catalogName, dbName, tableName) = table.split("/")
      (catalogName, dbName, tableName)
    }
    .filterNot(existingTableNames.contains)
    .toSet

  val values = valuesSet
    .map { case (catalogName, dbName, tableName) =>
      s"('$catalogName', '$dbName', '$tableName')"
    }
    .mkString(",")

  // Insert the new table names into the jobs table
  if (values.nonEmpty) {
    val insertStatement = s"INSERT INTO jobs (catalog_name, db_name, tbl_name) VALUES $values;"
    val statement = connection.createStatement()
    statement.executeUpdate(insertStatement)
    statement.close()
  }

  // Close the database connection
  connection.close()
}
