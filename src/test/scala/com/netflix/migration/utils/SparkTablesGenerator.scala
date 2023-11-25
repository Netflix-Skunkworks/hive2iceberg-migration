package com.netflix.migration.utils

import com.netflix.migration.data.{Job, MigrationConfig}
import com.netflix.migration.providers.{MetacatServiceProvider, MigrationConfigProvider, SparkSessionProvider}
import com.netflix.migration.services.{MetacatService, StorageService}
import com.netflix.migration.utils.MigrationConsts.TableFormat.TableFormat
import com.netflix.migration.utils.MigrationConsts.{DEFAULT_TABLE_OWNER, DO_NOT_DROP_TAG, DO_NOT_RENAME_TAG, HIVE_SUFFIX, ICEBERG_SUFFIX, TEST_CATALOG, TEST_DATABASE, TEST_TABLE_PREFIX, TEST_THREAD_POOL_SIZE}
import com.netflix.migration.utils.Utils.getMockJobObject
import org.apache.spark.sql
import org.apache.spark.sql.{Encoders, SparkSession}

import java.util
import java.util.concurrent.{Callable, Executors}
import scala.util.Random

class SparkTablesGenerator(
    numTables: Int,
    numRowsPerTable: Int,
    format: TableFormat,
    storageService: StorageService) {

  val spark: SparkSession = SparkSessionProvider.getSparkSession
  val metacatService: MetacatService = MetacatServiceProvider.getMetacatService
  val migrationConf: MigrationConfig = MigrationConfigProvider.getMigrationConfig

  def createTables(): Unit = {
    dropTables()
    val executor = Executors.newFixedThreadPool(TEST_THREAD_POOL_SIZE)
    val tasks = new util.ArrayList[Callable[Void]]()
    (1 to numTables).foreach { i =>
      val table = s"$TEST_CATALOG.$TEST_DATABASE.${TEST_TABLE_PREFIX}$i"
      tasks.add(new Callable[Void] {
        override def call(): Void = {
          createTable(table, format)
          writeNumRowsToTable(table)
          null
        }
      })
    }
    executor.invokeAll(tasks)
    executor.shutdown()
  }

  def createTable(table: String, format: TableFormat): Unit = {
    val table_format = format.toString.toLowerCase
    val tableParts = table.split("[.]")
    val (catalog_name, db_name, table_name) = (tableParts(0), tableParts(1), tableParts(2))
    spark.sql(
      s"CREATE TABLE IF NOT EXISTS $table " +
        s"(id INT, name STRING, date INT, hour INT) " +
        s"USING $table_format PARTITIONED BY (date, hour)")
    Utils.metacatService.setOwner(catalog_name, db_name, table_name, DEFAULT_TABLE_OWNER)
    println(s"Created test table: $table")
    metacatService.setTableTags(
      catalog_name,
      db_name,
      table_name,
      Set(DO_NOT_RENAME_TAG, DO_NOT_DROP_TAG))
    val job = getMockJobObject(catalog_name, db_name, table_name)
    storageService.enqueueJob(job)
  }

  def writeNumRowsToTable(table: String): Unit = {
    val encoder = Encoders.product[(Int, String, Int, Int)]
    val random = new Random()
    val data = spark
      .range(numRowsPerTable)
      .map { i =>
        (i.toInt, s"name_$i", 20230101, random.nextInt(10000000))
      }(encoder)
      .toDF("id", "name", "date", "hour")
      .repartition(spark.conf.get("spark.sql.shuffle.partitions").toInt * 2)
    writeToTable(table, data)
  }

  def writeToTable(table: String, data: sql.DataFrame): Unit = {
    data.write.insertInto(s"$table")
  }

  def dropTables(): Unit = {
    (1 to numTables).foreach { i =>
      dropTable(getMockJobObject(TEST_CATALOG, TEST_DATABASE, s"$TEST_TABLE_PREFIX$i"))
    }
  }

  def dropTable(job: Job): Unit = {
    try {
      if (metacatService.tableExists(job.catalogName, job.dbName, job.tblName)) {
        metacatService.unBlockTableWrites(job.catalogName, job.dbName, job.tblName)
        metacatService.forceDropTable(job.catalogName, job.dbName, job.tblName)
      }
      if (metacatService.tableExists(job.catalogName, job.dbName, job.tblName + HIVE_SUFFIX)) {
        metacatService.unBlockTableWrites(job.catalogName, job.dbName, job.tblName + HIVE_SUFFIX)
        metacatService.forceDropTable(job.catalogName, job.dbName, job.tblName + HIVE_SUFFIX)
      }
      if (metacatService.tableExists(job.catalogName, job.dbName, job.tblName + ICEBERG_SUFFIX)) {
        metacatService.unBlockTableWrites(
          job.catalogName,
          job.dbName,
          job.tblName + ICEBERG_SUFFIX)
        metacatService.forceDropTable(job.catalogName, job.dbName, job.tblName + ICEBERG_SUFFIX)
      }
    } catch {
      case e: Exception => // do nothing
    } finally {
      storageService.removeJob(job)
    }
  }
}
