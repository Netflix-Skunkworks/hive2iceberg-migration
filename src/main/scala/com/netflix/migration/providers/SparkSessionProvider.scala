package com.netflix.migration.providers

import org.apache.spark.sql.SparkSession

object SparkSessionProvider {
  private var spark: SparkSession = _

  /**
   * Initialize the SparkSession.
   *
   * @param sparkSession
   *   the SparkSession to be used for the migration
   */
  def init(sparkSession: SparkSession): Unit = spark = sparkSession
  
  def getSparkSession: SparkSession = spark
}
