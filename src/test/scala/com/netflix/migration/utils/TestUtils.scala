package com.netflix.migration.utils

import com.netflix.migration.providers.SparkSessionProvider
import org.apache.spark.sql.SparkSession

object TestUtils {

  val spark: SparkSession = SparkSessionProvider.getSparkSession

  def getHiveTableRowCount(table: String): Long = {
    spark.sql(s"SELECT COUNT(1) FROM $table").collect()(0).getLong(0)
  }

  def getIcebergTableRowCount(table: String): Long = {
    spark
      .sql(s"""
         |SELECT SUM(record_count)
         |FROM ${table}.partitions
         |""".stripMargin)
      .collect()(0)
      .getLong(0)
  }
}
