package com.netflix.migration.services

import com.netflix.migration.data.{Job, JobState, MigrationConfig}
import com.netflix.migration.providers.MigrationConfigProvider
import com.netflix.migration.services.StorageServiceImpl.{makeDbConnection, rsToJobs}
import com.netflix.migration.utils.MigrationConsts
import com.typesafe.scalalogging.StrictLogging
import play.api.libs.json.Json

import java.sql.{Connection, DriverManager, JDBCType, ResultSet}
import java.time.Instant

case class StorageServiceImpl(conf: MigrationConfig) extends StorageService with StrictLogging {

  /**
   * Get the job for the given table
   *
   * @param catalogName
   *   Name of the catalog
   * @param dbName
   *   Name of the database
   * @param tableName
   *   Name of the table
   * @return
   *   Job corresponding to table
   */
  override def getJob(catalogName: String, dbName: String, tableName: String): Job = {
    val conf: MigrationConfig = MigrationConfigProvider.getMigrationConfig
    val jobsQuery =
      s"""
         |SELECT *
         |FROM ${conf.queueName}
         |WHERE catalog_name = '$catalogName'
         |AND db_name = '$dbName'
         |AND tbl_name = '$tableName'
         |""".stripMargin
    getJobs(jobsQuery).head
  }

  /**
   * Get the list of jobs for the given query
   *
   * @param jobsQuery
   *   Jobs query
   * @return
   *   list of jobs
   */
  override def getJobs(jobsQuery: String): List[Job] = {
    var connection: Connection = null
    try {
      connection = makeDbConnection(conf)
      val statement = connection.createStatement
      val rs = statement.executeQuery(jobsQuery)
      rsToJobs(rs)
    } catch {
      case e: Exception =>
        logger.error(
          s"Failed to get jobs using query: $jobsQuery. Exception: ${e.printStackTrace()}")
        throw e
    } finally {
      connection.close()
    }
  }

  /**
   * Insert new job into the jobs queue
   *
   * @param job
   *   The job.
   */
  def enqueueJob(job: Job): Unit = {
    var connection: Connection = null
    try {
      connection = makeDbConnection(conf)
      val insertQuery =
        s"""
           |INSERT INTO ${conf.queueName} (catalog_name, db_name, tbl_name)
           |VALUES (?, ?, ?)
           |""".stripMargin
      val statement = connection.prepareStatement(insertQuery)

      val catalogNameStr =
        Option(job.catalogName).getOrElse(throw new RuntimeException("catalog is empty"))
      statement.setObject(1, catalogNameStr, JDBCType.VARCHAR)

      val databaseNameStr = if (Option(job.dbName).isEmpty) "" else job.dbName
      statement.setObject(2, databaseNameStr, JDBCType.VARCHAR)

      val tableNameStr = if (Option(job.tblName).isEmpty) "" else job.tblName
      statement.setObject(3, tableNameStr, JDBCType.VARCHAR)

      statement.executeUpdate
    } catch {
      case e: Exception =>
        logger.error(
          s"Encountered exception enqueuing Job: $job. Exception: ${e.printStackTrace()}")
        throw e
    } finally {
      connection.close()
    }
  }

  /**
   * Remove job from the jobs queue
   *
   * @param job
   *   The job.
   */
  def removeJob(job: Job): Unit = {
    var connection: Connection = null
    try {
      connection = makeDbConnection(conf)
      val deleteQuery =
        s"""
           |DELETE FROM ${conf.queueName}
           |WHERE catalog_name = ?
           |AND db_name = ?
           |AND tbl_name = ?
           |""".stripMargin
      val statement = connection.prepareStatement(deleteQuery)

      val catalogNameStr =
        Option(job.catalogName).getOrElse(throw new RuntimeException("catalog is empty"))
      statement.setObject(1, catalogNameStr, JDBCType.VARCHAR)

      val databaseNameStr = if (Option(job.dbName).isEmpty) "" else job.dbName
      statement.setObject(2, databaseNameStr, JDBCType.VARCHAR)

      val tableNameStr = if (Option(job.tblName).isEmpty) "" else job.tblName
      statement.setObject(3, tableNameStr, JDBCType.VARCHAR)

      statement.executeUpdate
    } catch {
      case e: Exception =>
        logger.error(
          s"Encountered exception removing Job: $job. Exception: ${e.printStackTrace()}")
        throw e
    } finally {
      connection.close()
    }
  }

  /**
   * Update the given job's column value.
   *
   * @param job
   *   The job.
   */
  override def updateJob(job: Job): Unit = {
    var connection: Connection = null
    try {
      connection = makeDbConnection(conf)
      val updateQuery =
        s"""
           |UPDATE ${conf.queueName}
           |SET stg_format = ?, data_category = ?, tbl_owners = ?, downstream_users = ?,
           |to_be_processed = ?, in_process = ?, state = ?, desired_state = ?,
           |initial_gap_days = ?, probation_gap_days = ?, comm_level1_date = ?,
           |comm_level2_date = ?, comm_level3_date = ?, shadow_watermark = ?,
           |migration_paused = ?, pause_reason = ?, run_id = ?, shadow_status = ?
           |WHERE task_id = ?
           |AND catalog_name = ?
           |AND db_name = ?
           |AND tbl_name = ?
           |""".stripMargin
      val statement = connection.prepareStatement(updateQuery)

      val stgFormatStr = if (Option(job.stgFormat).isEmpty) null else job.stgFormat
      statement.setObject(1, stgFormatStr, JDBCType.VARCHAR)

      val dataCategoryStr = if (Option(job.dataCategory).isEmpty) null else job.dataCategory
      statement.setObject(2, dataCategoryStr, JDBCType.VARCHAR)

      val tblOwnersJson = if (job.tblOwners.isEmpty) null else Json.toJson(job.tblOwners).toString
      statement.setObject(3, tblOwnersJson, JDBCType.VARCHAR)

      val tblDownstreamUsersJson =
        if (job.downstreamUsers.isEmpty) null else Json.toJson(job.downstreamUsers).toString
      statement.setObject(4, tblDownstreamUsersJson, JDBCType.VARCHAR)

      statement.setObject(5, job.toBeProcessed, JDBCType.BOOLEAN)
      statement.setObject(6, job.inProcess, JDBCType.BOOLEAN)

      val jobState =
        if (Option(job.state.toString).isEmpty || job.state == JobState.Undefined)
          JobState.Undefined
        else job.state.toString
      statement.setObject(7, jobState.toString, JDBCType.VARCHAR)

      val jobDesiredState =
        if (Option(job.desiredState.toString).isEmpty || job.desiredState == JobState.Undefined)
          JobState.Undefined
        else job.desiredState
      statement.setObject(8, jobDesiredState.toString, JDBCType.VARCHAR)

      statement.setObject(9, job.initialGapInDays, JDBCType.INTEGER)
      statement.setObject(10, job.probationGapInDays, JDBCType.INTEGER)

      val jobCommLevel1Date =
        if (job.commLevel1Date != -1)
          java.sql.Timestamp.from(Instant.ofEpochMilli(job.commLevel1Date))
        else null
      statement.setTimestamp(11, jobCommLevel1Date)

      val jobCommLevel2Date =
        if (job.commLevel2Date != -1)
          java.sql.Timestamp.from(Instant.ofEpochMilli(job.commLevel2Date))
        else null
      statement.setTimestamp(12, jobCommLevel2Date)

      val jobCommLevel3Date =
        if (job.commLevel3Date != -1)
          java.sql.Timestamp.from(Instant.ofEpochMilli(job.commLevel3Date))
        else null
      statement.setTimestamp(13, jobCommLevel3Date)

      statement.setObject(14, job.shadowWatermark, JDBCType.BIGINT)
      statement.setObject(15, job.migrationPaused, JDBCType.BOOLEAN)
      statement.setObject(16, job.pauseReason, JDBCType.VARCHAR)
      statement.setObject(17, job.runId, JDBCType.VARCHAR)

      val jobShadowStatusStr = Option(job.shadowStatus).orNull
      statement.setObject(18, jobShadowStatusStr, JDBCType.VARCHAR)

      statement.setObject(19, job.id, JDBCType.INTEGER)

      val catalogNameStr = if (Option(job.catalogName).isEmpty) "" else job.catalogName
      statement.setObject(20, catalogNameStr, JDBCType.VARCHAR)

      val databaseNameStr = if (Option(job.dbName).isEmpty) "" else job.dbName
      statement.setObject(21, databaseNameStr, JDBCType.VARCHAR)

      val tableNameStr = if (Option(job.tblName).isEmpty) "" else job.tblName
      statement.setObject(22, tableNameStr, JDBCType.VARCHAR)

      statement.executeUpdate
    } catch {
      case e: Exception =>
        logger.error(
          s"Encountered exception updating Job: $job. Exception: ${e.printStackTrace()}")
        throw e
    } finally {
      connection.close()
    }
  }
}

object StorageServiceImpl {

  def makeDbConnection(conf: MigrationConfig): Connection = {
    val dbServer =
      if (conf.dbEnv.equals(MigrationConsts.PROD_ENV))
        MigrationConsts.JOBS_DB_PROD_URL
      else MigrationConsts.JOBS_DB_TEST_URL
    val url =
      s"jdbc:mysql://$dbServer/${conf.dbName}?useUnicode=true&connectTimeout=30000&characterEncoding=latin1&autoReconnect=true&sessionVariables=@@innodb_lock_wait_timeout=300&enabledTLSProtocols=TLSv1.2"
    val driver = "com.mysql.cj.jdbc.Driver"
    val username = MigrationConsts.MYSQL_USER
    val password = MigrationConsts.MYSQL_PASS
    Class.forName(driver)
    DriverManager.getConnection(url, username, password)
  }

  def rsToJobs(rs: ResultSet): List[Job] = {
    var res: List[Job] = List()
    while (rs.next) {
      val id = Option(rs.getInt("task_id")).getOrElse(-1)
      val catalog = Option(rs.getString("catalog_name")).getOrElse("")
      val db = Option(rs.getString("db_name")).getOrElse("")
      val tbl = Option(rs.getString("tbl_name")).getOrElse("")
      val storageFormat = Option(rs.getString("stg_format")).getOrElse("")
      val dataCategory = Option(rs.getString("data_category")).getOrElse("")
      val tableOwners =
        Option(rs.getString("tbl_owners")).map(Json.parse(_).as[Set[String]]).getOrElse(Set())
      val downstreamUsers = Option(rs.getString("downstream_users"))
        .map(Json.parse(_).as[Set[String]])
        .getOrElse(Set())
      val toBeProcessed = Option(rs.getInt("to_be_processed")).getOrElse(0)
      val inProcess = Option(rs.getInt("in_process")).getOrElse(0)
      val state =
        Option(rs.getString("state")).map(JobState.withName).getOrElse(JobState.Undefined)
      val desired =
        Option(rs.getString("desired_state")).map(JobState.withName).getOrElse(JobState.Undefined)
      val initialGapInDays =
        Option(rs.getInt("initial_gap_days")).getOrElse(0)
      val probationGapInDays =
        Option(rs.getInt("probation_gap_days")).getOrElse(0)
      val commLevel1Date =
        Option(rs.getTimestamp("comm_level1_date")).fold(-1L)(_.toInstant.toEpochMilli)
      val commLevel2Date =
        Option(rs.getTimestamp("comm_level2_date")).fold(-1L)(_.toInstant.toEpochMilli)
      val commLevel3Date =
        Option(rs.getTimestamp("comm_level3_date")).fold(-1L)(_.toInstant.toEpochMilli)
      val shadowWatermark = Option(rs.getLong("shadow_watermark")).getOrElse(-1L)
      val migrationPaused = Option(rs.getInt("migration_paused")).getOrElse(0)
      val pauseReason = Option(rs.getString("pause_reason")).getOrElse("None")
      val runId = Option(rs.getString("run_id")).orNull
      val shadowStatus = Option(rs.getString("shadow_status")).orNull
      val createdAt = Option(rs.getTimestamp("created_at")).fold(-1L)(_.toInstant.toEpochMilli)
      val lastUpdatedTime =
        Option(rs.getTimestamp("last_updated_time")).fold(-1L)(_.toInstant.toEpochMilli)
      res = new Job(
        id,
        catalog,
        db,
        tbl,
        storageFormat,
        dataCategory,
        tableOwners,
        downstreamUsers,
        toBeProcessed,
        inProcess,
        state,
        desired,
        initialGapInDays,
        probationGapInDays,
        commLevel1Date,
        commLevel2Date,
        commLevel3Date,
        shadowWatermark,
        migrationPaused,
        pauseReason,
        runId,
        shadowStatus,
        createdAt,
        lastUpdatedTime) :: res
    }
    res
  }
}
