package com.netflix.migration.utils

import com.netflix.metacat.common.dto.TableDto
import com.netflix.migration.data.{Job, JobState, MigrationCommandMode, MigrationConfig}
import com.netflix.migration.modes.Shadower
import com.netflix.migration.providers.{IcebergTableServiceProvider, MetacatServiceProvider, MigrationConfigProvider, NdcServiceProvider, SparkSessionProvider}
import com.netflix.migration.services.{IcebergTableService, MetacatService, NdcService, StorageService, StorageServiceImpl}
import com.netflix.migration.utils.MigrationConsts.DataCategory.DataCategory
import com.netflix.migration.utils.MigrationConsts.{ARCHIVED_SUFFIX, DO_NOT_DROP_TAG, DO_NOT_RENAME_TAG, HIVE_ARCHIVED_TAG, HIVE_SUFFIX, HIVE_TIMESTAMP_COLUMN_NAME, ICEBERG_SUFFIX, PROD_ENV, PauseReason, REVERT_SUFFIX}
import com.netflix.migration.utils.MigrationRunnerUtils.retry
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.nio.file.Files
import java.security.MessageDigest
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.collection.JavaConverters.{asScalaIteratorConverter, iterableAsScalaIterableConverter}
import scala.collection.mutable

object Utils extends StrictLogging {

  val migrationConf: MigrationConfig = MigrationConfigProvider.getMigrationConfig
  var spark: SparkSession = SparkSessionProvider.getSparkSession
  var metacatService: MetacatService = MetacatServiceProvider.getMetacatService
  var ndcService: NdcService = NdcServiceProvider.getNdcService
  var icebergTableService: IcebergTableService =
    IcebergTableServiceProvider.getIcebergTableService

  type transition = Job => Unit

  private val stageChangeActionMap: Map[(JobState.JobState, JobState.JobState), transition] =
    Map(
      (JobState.Ready, JobState.WritesBlocked) -> blockWrites,
      (JobState.WritesBlocked, JobState.IcebergReady) -> migrateTableIceberg,
      (JobState.IcebergReady, JobState.IcebergPrimary) -> makeIcebergPrimary,
      (JobState.IcebergPrimary, JobState.WritesUnblocked) -> unblockWrites,
      (JobState.WritesUnblocked, JobState.WritesBlocked) -> blockWrites,
      (JobState.WritesBlocked, JobState.SyncHive) -> syncHiveFromIceberg,
      (JobState.SyncHive, JobState.HivePrimary) -> makeHivePrimary,
      (JobState.HivePrimary, JobState.WritesUnblocked) -> unblockWrites,
      (JobState.WritesUnblocked, JobState.IcebergDropped) -> dropIceberg,
      (JobState.WritesUnblocked, JobState.HiveDropped) -> dropHive)

  /**
   * @param job
   * @return
   */
  def getJobNextAction(job: Job): Option[transition] = {
    stageChangeActionMap.get((job.state, job.desiredState))
  }

  /**
   * Update the state of a job entity.
   *
   * @param job
   *   the job entity to be updated
   * @param cur
   *   the current state of the job
   * @param desired
   *   the desired state of the job
   */
  private def updateJobState(
      job: Job,
      cur: JobState.JobState,
      desired: JobState.JobState): Unit = {
    job.desiredState = desired
    job.state = cur
  }

  /**
   * Block writes for the specified job
   *
   * @param job
   *   the job for which the writes needs to be blocked
   */
  private def blockWrites(job: Job): Unit = {
    val fullTableName = s"${job.catalogName}/${job.dbName}/${job.tblName}"
    println(s"Blocking writes for table: $fullTableName")
    metacatService.blockTableWrites(job.catalogName, job.dbName, job.tblName)
    println(s"Successfully blocked writes for table: $fullTableName")

    if (migrationConf.commandMode == MigrationCommandMode.REVERTER) {
      updateJobState(job, JobState.WritesBlocked, JobState.SyncHive)
    } else {
      updateJobState(job, JobState.WritesBlocked, JobState.IcebergReady)
    }
  }

  /**
   * Unblock writes for the specified job
   *
   * @param job
   *   the job for which the writes needs to be unblocked
   */
  private def unblockWrites(job: Job): Unit = {
    val tblName = job.tblName
    val hiveTblName = job.tblName + HIVE_SUFFIX
    metacatService.unBlockTableWrites(job.catalogName, job.dbName, tblName)
    if (migrationConf.dbEnv == PROD_ENV) {
      metacatService.setStrictSecure(job.catalogName, job.dbName, tblName)
    }
    metacatService.unBlockTableWrites(job.catalogName, job.dbName, hiveTblName)
    metacatService.setTableTag(job.catalogName, job.dbName, hiveTblName, HIVE_ARCHIVED_TAG)

    if (migrationConf.commandMode == MigrationCommandMode.REVERTER) {
      updateJobState(job, JobState.WritesUnblocked, JobState.IcebergDropped)
    } else {
      // for now transition to hive dropped is manual for safety, can be automated down the line
      updateJobState(job, JobState.WritesUnblocked, JobState.WritesUnblocked)
    }
  }

  /**
   * Returns the qualified table name for a job based on the Spark version
   *
   * @param spark
   *   SparkSession
   * @param job
   *   Job
   * @return
   *   String
   */
  private def jobQualTblNameForSpark(spark: SparkSession, job: Job): String = {
    val base = s"${job.dbName}.${job.tblName}"
    if (spark.version >= "2.4") return s"${job.catalogName}." + base
    if (job.catalogName == "prodhive") base else null
  }

  def getSparkSession(): SparkSession = {
    if (spark == null) {
      spark = buildSparkSession()
    }
    spark
  }

  /**
   * Build a SparkSession with the appropriate configurations
   *
   * @return
   *   The SparkSession object
   */
  private def buildSparkSession(): SparkSession = {
    val sparkConf = initializeSparkConf()
    if (migrationConf.runLocally) {
      SparkSession
        .builder()
        .master("local[*]")
        .config(initializeLocalSparkConfLocal(sparkConf))
        .enableHiveSupport()
        .getOrCreate()
    } else {
      SparkSession
        .builder()
        .config(sparkConf)
        .enableHiveSupport()
        .getOrCreate()
    }
  }

  /**
   * Initialize the SparkConf with settings specific to the Spark Iceberg Migration job
   *
   * @return
   *   The SparkConf object
   */
  private def initializeSparkConf(): SparkConf = {
    new SparkConf(true)
      .setAppName("Spark Iceberg Migration")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.sql.shuffle.partitions", "1500")
      .set("spark.speculation", "false")
      .set("spark.task.maxFailures", "30") // default=4
      .set("spark.network.timeout", "900")
      .set("spark.shuffle.io.maxRetries", "30") // default=3
      .set("spark.reducer.maxReqsInFlight", "10")
      .set("spark.rpc.message.maxSize", "2047")
      .set("spark.executor.memoryOverhead", "3g")
      .set("spark.driver.memory", "15g")
      .set("spark.executor.memory", "15g")
      .set("spark.executor.cores", "8")
      .set("spark.task.cpus", "2")
      .set(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
  }

  /**
   * Initialize the SparkConf with settings specific to a local environment
   *
   * @param sparkConf
   *   SparkConf object to be modified
   * @return
   *   The modified SparkConf object
   */
  def initializeLocalSparkConfLocal(sparkConf: SparkConf): SparkConf = {
    val tmp_dir = Files.createTempDirectory("MigrationTooling-").toFile.getAbsolutePath
    sparkConf
      .set(
        "spark.hadoop.bdp.s3.credentials-provider",
        "com.netflix.hadoop.aws.ConfigurableCredentialsProvider")
      .set("spark.hadoop.bdp.s3.staging-directory", s"$tmp_dir/s3")
      .set("spark.hadoop.bdp.s3.use-instance-credentials", "false")
      .set("spark.hadoop.fs.defaultFS", s"file:$tmp_dir")
      .set("spark.hadoop.hive-metastore.host", "http://hive-metastore-host-url")
  }

  /**
   * Migrate the table associated with the given job to Iceberg
   *
   * @param job
   *   Job object containing information about the table and its location
   */
  private def migrateTableIceberg(job: Job): Unit = {
    if (!isHiveTable(job.catalogName, job.dbName, job.tblName)) {
      metacatService.unBlockTableWrites(job.catalogName, job.dbName, job.tblName)
      job.migrationPaused = 1
      job.pauseReason = PauseReason.IS_ALREADY_ICEBERG.toString
      job.state = JobState.Ready
      job.desiredState = JobState.WritesBlocked
      @transient lazy val storageService: StorageService = StorageServiceImpl(migrationConf)
      storageService.updateJob(job)
      return
    }

    val jobQualTblName = jobQualTblNameForSpark(spark, job)
    val icebergJobQualTblName = jobQualTblName + ICEBERG_SUFFIX
    if (jobQualTblName == null) {
      logger.warn(s"can only migrate prodhive for this spark version. Table: $jobQualTblName")
      updateJobState(job, JobState.IcebergReady, JobState.IcebergReady)
      return
    }

    val icebergTableName = job.tblName + ICEBERG_SUFFIX
    val icebergQName =
      StringUtils.getTableQualifiedName(job.catalogName, job.dbName, icebergTableName)
    // If the iceberg table already exists, drop it.
    if (metacatService.tableExists(job.catalogName, job.dbName, icebergTableName)) {
      logger.warn(s"Iceberg table: $icebergQName already exists. Attempting to drop it..")
      metacatService.deleteTable(job.catalogName, job.dbName, icebergTableName)
    }

    var hasTimestampColumn = false
    val tableDtoOpt = metacatService.getTable(job.catalogName, job.dbName, job.tblName)
    tableDtoOpt match {
      case Some(tableDto) if Utils.tableHasTimestampColumn(tableDto) =>
        hasTimestampColumn = true
      case _ => // Do nothing
    }

    try {
      if ((job.stgFormat.equals("parquet") && !job.dbName.startsWith("ae_") && !hasTimestampColumn) ||
        isHiveTableEmpty(job)) {
        spark.sql(s"SNAPSHOT TABLE $jobQualTblName AS $icebergJobQualTblName USING iceberg")
      } else {
        throw new org.apache.spark.SparkException(
          "Cannot convert table with non-Parquet partitions")
      }
    } catch {
      case e: org.apache.spark.SparkException =>
        job.stgFormat = MigrationConsts.HIVE_CSV_TEXT
        if (e.getMessage.contains("Cannot convert table with non-Parquet partitions") ||
          e.getMessage.contains("is not a Parquet file")) {
          spark.sql(s"CREATE TABLE $icebergJobQualTblName LIKE $jobQualTblName USING iceberg")
          spark.sql(s"ALTER TABLE $icebergJobQualTblName " +
            s"SET TBLPROPERTIES ('spark.behavior.compatibility'='true','migrated-from-hive'='true')")
          val shadower = new Shadower
          shadower.copyDataSrcToDst(jobQualTblName, icebergJobQualTblName)
        } else {
          throw e // if the exception message is not the one we expect, rethrow the exception
        }
    }
    updateJobState(job, JobState.IcebergReady, JobState.IcebergPrimary)
  }

  /**
   * Set the watermark snapshot ID for the given job's table in Hive
   *
   * @param job
   *   Job object containing information about the table and its location
   */
  private def setWatermarkSnapshotId(job: Job): Unit = {
    val tblName = getFullyQualifiedTableName(job)
    val hiveFullTableName = tblName + HIVE_SUFFIX
    val hiveTableName = job.tblName + HIVE_SUFFIX
    val icebergTableName = tblName
    try {
      val currentSnapshotId = getCurrentSnapshotId(icebergTableName)
      // Since renameTable carries over tags, hiveName table will also have writes blocked
      metacatService.unBlockTableWrites(job.catalogName, job.dbName, hiveTableName)
      spark.sql(
        s"ALTER TABLE $hiveFullTableName SET TBLPROPERTIES('watermark_snapshot_id' = $currentSnapshotId)")
      metacatService.blockTableWrites(job.catalogName, job.dbName, hiveTableName)
    } catch {
      case _ =>
        logger.warn(
          "Iceberg table is unaccessible, possible migrated from legacy Hive table with STRICT permission issue")
    }
  }

  private def syncHiveFromIceberg(job: Job): Unit = {
    val shadower = new Shadower
    shadower.shadowJob(job)
    updateJobState(job, JobState.SyncHive, JobState.HivePrimary)
  }

  /**
   * Make Hive primary for the given job
   *
   * @param job
   *   Job object containing information about the table and its location
   */
  private def makeHivePrimary(job: Job): Unit = {
    val tblName = job.tblName
    val hiveName = tblName + HIVE_SUFFIX
    val icebergName = tblName + ICEBERG_SUFFIX
    metacatService.renameTable(job.catalogName, job.dbName, tblName, icebergName)
    metacatService.renameTable(job.catalogName, job.dbName, hiveName, tblName)
    updateJobState(job, JobState.HivePrimary, JobState.WritesUnblocked)
  }

  /**
   * Make Iceberg primary for the given job
   *
   * @param job
   *   Job object containing information about the table and its location
   */
  private def makeIcebergPrimary(job: Job): Unit = {
    val tblName = job.tblName
    val hiveName = tblName + HIVE_SUFFIX
    val icebergName = tblName + ICEBERG_SUFFIX
    val hiveQName = StringUtils.getTableQualifiedName(job.catalogName, job.dbName, hiveName)
    val icebergQName = StringUtils.getTableQualifiedName(job.catalogName, job.dbName, icebergName)

    // Ensure hive table doesn't already exist (this could be a retry)
    if (!metacatService.tableExists(job.catalogName, job.dbName, hiveName)) {
      metacatService.renameTable(job.catalogName, job.dbName, tblName, hiveName)
    } else {
      logger.warn(s"Hive Table $hiveQName already exists. Skipping rename.")
    }

    // On a retry, the iceberg table won't exist. So make sure it exists the first time.
    if (metacatService.tableExists(job.catalogName, job.dbName, icebergName)) {
      metacatService.renameTable(job.catalogName, job.dbName, icebergName, tblName)
    } else {
      logger.warn(s"Iceberg Table $icebergQName does not exist. Skipping rename.")
    }

    setIcebergTableACLs(job)
    copyHiveDefinitionMetadata(job.catalogName, job.dbName, hiveName, tblName)
    copyHiveClassificationMetadata(job.catalogName, job.dbName, hiveName, tblName)
    setWatermarkSnapshotId(job)
    updateJobState(job, JobState.IcebergPrimary, JobState.WritesUnblocked)
  }

  private def copyHiveClassificationMetadata(
      catalogName: String,
      dbName: String,
      hiveTableName: String,
      icebergTableName: String): Unit = {
    val srcMetadataResponseDto = ndcService.getMetadata(catalogName, dbName, hiveTableName)
    if (srcMetadataResponseDto != null) {
      val dstMetadataResponseDto = ndcService.getMetadata(catalogName, dbName, icebergTableName)
      val metadataDto = ndcService.createMetadataDto(srcMetadataResponseDto)
      if (dstMetadataResponseDto != null) {
        metadataDto.setId(dstMetadataResponseDto.getId)
        metadataDto.setName(dstMetadataResponseDto.getName)
        metadataDto.setLocation(dstMetadataResponseDto.getLocation)
        metadataDto.setSourceLink(dstMetadataResponseDto.getSourceLink)
      }
      ndcService.setMetadata(List(metadataDto))
    }
  }

  private def copyHiveDefinitionMetadata(
      catalogName: String,
      dbName: String,
      hiveTableName: String,
      icebergTableName: String): Unit = {
    val hiveTableDtoOpt = metacatService.getTable(catalogName, dbName, hiveTableName)
    if (hiveTableDtoOpt.isEmpty) {
      logger.warn(s"Hive table: $hiveTableName doesn't exist. Skipping copy definition metadata.")
      return
    }
    val hiveTableDto = hiveTableDtoOpt.get
    val hiveDefinitionMetadata = hiveTableDto.getDefinitionMetadata
    if (hiveDefinitionMetadata == null) {
      logger.warn(s"Hive table: $hiveTableName definition metadata is empty.")
    }

    val icebergTableDtoOpt = metacatService.getTable(catalogName, dbName, icebergTableName)
    if (icebergTableDtoOpt.isEmpty) {
      logger.warn(
        s"Iceberg table: $icebergTableName doesn't exist. Skipping copy definition metadata.")
      return
    }
    val icebergTableDto = icebergTableDtoOpt.get
    if (getTagsFromTableMetadata(icebergTableDto)
        .getOrElse(Set())
        .contains(MigrationConsts.ICEBERG_MIGRATION_DO_NOT_MODIFY_TAG)) {
      logger.warn(s"Iceberg table: $icebergTableName already contains definitionMetadata " +
        s"and tag ${MigrationConsts.ICEBERG_MIGRATION_DO_NOT_MODIFY_TAG}. Skipping copy definition metadata.")
      return
    }

    try {
      icebergTableDto.setDefinitionMetadata(hiveDefinitionMetadata)
      setDataMigratedLocation(hiveTableDto, icebergTableDto)
      metacatService.updateTable(catalogName, dbName, icebergTableName, icebergTableDto)
      var tagsSet: Set[String] = getTagsFromTableMetadata(hiveTableDto).getOrElse(Set())
      tagsSet = tagsSet ++ Set(MigrationConsts.MIGRATED_FROM_HIVE_TAG)
      metacatService.setTableTags(catalogName, dbName, icebergTableName, tagsSet)
      logger.info(
        s"Successfully copied definition from hive table: $hiveTableName to $icebergTableName")
    } catch {
      case e: Exception =>
        logger.error(
          s"Failed to update iceberg table: $icebergTableName exists in metacat. Exception: $e")
        throw e
    }
  }

  def setDataMigratedLocation(hiveTableDto: TableDto, icebergTableDto: TableDto): Unit = {
    if (MigrationRunnerUtils
        .getMetacatFieldValue(icebergTableDto, MigrationConsts.METACAT_DATA_MIGRATED_PROPERTY)
        .isDefined) {
      logger.warn(s"Migrated data location already set for ${icebergTableDto.getName}")
      return
    }

    // Get the data location from the hive table
    val hiveTableLocation = hiveTableDto.getDataUri
    if (hiveTableLocation == null) {
      throw new RuntimeException(
        s"Could not retrieve data location for metacat table: ${hiveTableDto.getName}")
    }

    MigrationRunnerUtils.setMetacatFieldValue(
      icebergTableDto,
      MigrationConsts.METACAT_DATA_MIGRATED_PROPERTY,
      hiveTableLocation)
  }

  /**
   * Grant the appropriate secure Iceberg permissions to the original hive table owner.
   *
   * @param job
   *   The migration job.
   */
  private def setIcebergTableACLs(job: Job): Unit = {
    if (!migrationConf.setIcebergAcls) {
      logger.warn(
        s"setIcebergAcls conf set to false. Skipping granting ACLs for table: ${job.tblName}")
      return
    }
    // Grant the permissions using the creds of the owner of this genie job
    // Since they'll have ownership of the iceberg table too.
    val jobUserName = spark.sparkContext.sparkUser
    try {
      retry(2)(icebergTableService.setGrants(jobUserName, job.catalogName, job.dbName, job.tblName))
    } catch {
      case e =>
        if (e.getMessage.contains("Access Denied")) {
          logger.warn(
            "Iceberg table is unaccessible, possible migrated from legacy Hive table with STRICT permission issue")
        }
    }
  }

  /**
   * Drops the Iceberg table associated with the given job
   *
   * @param job
   *   Job object containing information about the table and its location
   */
  private def dropIceberg(job: Job): Unit = {
    val tblName = job.tblName + ICEBERG_SUFFIX
    val tblQName = StringUtils.getTableQualifiedName(job.catalogName, job.dbName, tblName)
    val revertTblName = tblName + REVERT_SUFFIX
    val revertQTblName = StringUtils.getTableQualifiedName(job.catalogName, job.dbName, tblName)
    if (!metacatService.tableExists(job.catalogName, job.dbName, revertTblName)) {
      metacatService.unBlockTableWrites(job.catalogName, job.dbName, tblName)
      metacatService.renameTable(job.catalogName, job.dbName, tblName, revertTblName)
    } else {
      logger.warn(s"Table $revertQTblName already exists. Skipping rename of $tblQName")
    }
    updateJobState(job, JobState.IcebergDropped, JobState.IcebergDropped)
  }

  /**
   * Drops the Hive table associated with the given job
   *
   * @param job
   *   Job object containing information about the table and its location
   */
  private def dropHive(job: Job): Unit = {
    val tblQualifiedName = jobQualTblNameForSpark(spark, job) + HIVE_SUFFIX
    val tblName = job.tblName + HIVE_SUFFIX
    // Archive Hive table in all cases for correctness reasons
    // Keep this change till appropriate changes are made to Janitor.
    if (migrationConf.archiveHive || !migrationConf.archiveHive) {
      val archivedTblName = tblName + ARCHIVED_SUFFIX
      val archivedQualifiedTblName = tblQualifiedName + ARCHIVED_SUFFIX
      if (!metacatService.tableExists(job.catalogName, job.dbName, archivedTblName)) {
        metacatService.unBlockTableWrites(job.catalogName, job.dbName, tblName)
        metacatService.renameTable(job.catalogName, job.dbName, tblName, archivedTblName)
        metacatService.appendTableTags(
          job.catalogName,
          job.dbName,
          archivedTblName,
          Set(HIVE_ARCHIVED_TAG, DO_NOT_RENAME_TAG, DO_NOT_DROP_TAG))
      } else {
        logger.warn(
          s"Table $archivedQualifiedTblName already exists. Skipping rename of $tblQualifiedName")
      }
    } else {
      spark.sql(s"DROP TABLE IF EXISTS $tblName")
      logger.info(s"Successfully dropped table: $tblName")
    }
    updateJobState(job, JobState.HiveDropped, JobState.HiveDropped)
  }

  /**
   * Get the table owners for a given job
   *
   * @param job
   *   Job object containing information about the table and its location
   * @return
   *   Set of table owners as a string
   */
  def getTableOwners(job: Job): Set[String] = {
    var tableOwners: Set[String] = Set()
    tableOwners = ndcService.getTableOwners(job.catalogName, job.dbName, job.tblName).toSet
    if (tableOwners == null || tableOwners.isEmpty) {
      val ndc_query =
        s"""
           |SELECT verifiedTechnicalContacts
           |FROM bdp.ndc_metadata
           |WHERE nameDetails['catalogName'] = '${job.catalogName}'
           |AND nameDetails['database'] = '${job.dbName}'
           |AND nameDetails['table'] = '${job.tblName}'
           |AND dateint = DATE_FORMAT(DATE_SUB(CURRENT_DATE(), 1), 'YYYYMMdd')
           |""".stripMargin
      var result = spark.sql(ndc_query)
      val firstResult = result.head(1)
      if (!firstResult.isEmpty) {
        tableOwners ++= firstResult.head.getAs[Seq[String]]("verifiedTechnicalContacts").toSet
      } else {
        val metacat_query =
          s"""
             |SELECT CONCAT(nf_json_extract_scalar(other_properties['current'], '$$.definitionMetadata.owner.userId'), '@netflix.com')
             |AS table_owner
             |FROM bdp_metacat_table_events
             |WHERE other_properties['name'] = '${job.catalogName}/${job.dbName}/${job.tblName}'
             |AND other_properties['name'] IS NOT NULL
             |AND other_properties['name'] != ''
             |AND other_properties['current'] IS NOT NULL
             |AND other_properties['current'] != ''
             |LIMIT 1
             |""".stripMargin
        result = spark.sql(metacat_query)
        val firstResult = result.head(1)
        if (!firstResult.isEmpty) {
          tableOwners += firstResult.head.getAs[String]("table_owner")
        }
      }
    }
    tableOwners
  }

  /**
   * Get the downstream users for a given job
   *
   * @param job
   *   Job object containing information about the table and its location
   * @return
   *   Set of downstream users as a string
   */
  def getDownstreamUsers(job: Job): Set[String] = {
    val query =
      s"""
         |SELECT COLLECT_SET(DISTINCT(user_id)) AS downstream_user
         |FROM lineage.lineage_daily_agg
         |WHERE source_name = '${job.catalogName}/${job.dbName}/${job.tblName}'
         |AND operation = 'read'
         |""".stripMargin
    val result = spark.sql(query)
    var downstreamUsers: Set[String] = Set()
    val firstResult = result.head(1)
    if (!firstResult.isEmpty) {
      downstreamUsers ++= firstResult.head.getAs[Seq[String]]("downstream_user").toSet
    }
    var returnDownstreamUsers: Set[String] = Set()
    for (user <- downstreamUsers) {
      if (user.contains("@netflix.com")) {
        returnDownstreamUsers += user
      } else {
        returnDownstreamUsers += s"$user@netflix.com"
      }
    }
    returnDownstreamUsers
  }

  /**
   * Get the storage format of a given table of the job
   *
   * @param job
   *   Job object containing information about the table and its location
   * @return
   *   Storage format of the table as a string (e.g. "parquet" or "csv/text")
   */
  def getTableStorageFormat(job: Job): String = {
    metacatService.getTableStorageFormat(job.catalogName, job.dbName, job.tblName)
  }

  /**
   * Get table data category
   *
   * @param job
   */
  def getDataCategory(job: Job): DataCategory = {
    ndcService.getDataCategory(job.catalogName, job.dbName, job.tblName)
  }

  /**
   * Get the fully qualified table name of a given job
   *
   * @param job
   *   Job object containing information about the table and its location
   * @return
   *   Fully qualified table name as a string (e.g. "catalogName.dbName.tblName")
   */
  def getFullyQualifiedTableName(job: Job): String = {
    s"${job.catalogName}.${job.dbName}.${job.tblName}"
  }

  /**
   * Get the snapshot id of a current snapshot of the given iceberg table
   *
   * @param icebergTableName
   *   Name of the iceberg table as a string
   * @return
   *   Snapshot id of the current snapshot
   */
  private def getCurrentSnapshotId(icebergTableName: String): Any = {
    spark.sql(s"REFRESH TABLE $icebergTableName")
    spark
      .sql(
        s"SELECT snapshot_id FROM $icebergTableName.snapshots ORDER BY committed_at DESC LIMIT 1")
      .first()
      .get(0)
  }

  /**
   * Check if the given table is an Hive table
   *
   * @param catalogName
   *   Name of the catalog
   * @param dbName
   *   Name of the database
   * @param tableName
   *   Table name to check
   * @return
   *   Boolean indicating if the table is an Hive table
   */
  def isHiveTable(catalogName: String, dbName: String, tableName: String): Boolean = {
    if (!metacatService.tableExists(catalogName, dbName, tableName)) {
      return false
    }
    val tableDto = metacatService.getTable(catalogName, dbName, tableName).get
    Option(tableDto.getMetadata.get("table_type")) match {
      case Some(tableType) => !tableType.toLowerCase.contains("iceberg")
      case None => Option(tableDto.getSerde.getUri).exists(_.contains("hive"))
    }
  }

  /**
   * Get the snapshot id of the watermark snapshot of the given hive table
   *
   * @param hiveTableName
   *   Name of the hive table as a string
   * @return
   *   Snapshot id of the watermark snapshot or -1 if it does not exist
   */
  private def getWatermarkSnapshotId(hiveTableName: String): Any = {
    spark.sql(s"REFRESH TABLE $hiveTableName")
    spark
      .sql(s"SHOW TBLPROPERTIES $hiveTableName('watermark_snapshot_id')")
      .first()
      .get(1) match {
      case s: String if s.startsWith("Table") => -1
      case v => v
    }
  }

  /**
   * Check if the hive and iceberg tables of a given job are in sync
   *
   * @param job
   *   Job object containing information about the table and its location
   * @return
   *   Boolean indicating if the tables are in sync
   */
  def hiveIcebergTablesInSync(job: Job): Boolean = {
    val tableName = getFullyQualifiedTableName(job)
    val hiveTableName = s"$tableName$HIVE_SUFFIX"
    val icebergTableName = tableName
    val currentSnapshotId = getCurrentSnapshotId(icebergTableName)
    val watermarkSnapshotId = getWatermarkSnapshotId(hiveTableName)
    watermarkSnapshotId.==(currentSnapshotId)
  }

  private def getTagsFromTableMetadata(tableDto: TableDto): Option[Set[String]] = {
    if (tableDto.getDefinitionMetadata != null) {
      val tagsNode = tableDto.getDefinitionMetadata.path("tags")
      if (tagsNode != null &&
        tagsNode.isArray &&
        tagsNode.size() > 0) {
        val tagsSet: mutable.HashSet[String] = mutable.HashSet()
        // Get the existing set of tags and append to them since
        // the tags API is a replace all tags API.
        for (tag <- tagsNode.asScala) {
          tagsSet.add(tag.asText().trim)
        }
        return Some(tagsSet.toSet)
      }
    }
    None
  }

  def tableHasTimestampColumn(tableDto: TableDto): Boolean = {
    val metacatTableSchema = tableDto.getFields
    if (metacatTableSchema != null &&
      metacatTableSchema.size() > 0) {
      return metacatTableSchema
        .iterator()
        .asScala
        .toStream
        .count(f => {
          !f.getName.equals(HIVE_TIMESTAMP_COLUMN_NAME) &&
          f.getSource_type != null &&
          f.getSource_type.toLowerCase.contains(HIVE_TIMESTAMP_COLUMN_NAME) &&
          !f.getSource_type.toLowerCase.contains(HIVE_TIMESTAMP_COLUMN_NAME + ":")
        }) > 0
    }
    false
  }

  /**
   * Get mock Job object
   *
   * @param catalog_name
   *   catalog name of the table
   * @param db_name
   *   database name of the table
   * @param table_name
   *   name of the table
   * @return
   *   Boolean indicating if the tables are in sync
   */
  def getMockJobObject(catalog_name: String, db_name: String, table_name: String): Job = {
    new Job(
      null,
      catalog_name,
      db_name,
      table_name,
      "",
      dataCategory = "",
      null,
      null,
      0,
      0,
      JobState.Undefined,
      JobState.Undefined,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      "",
      null,
      null,
      0,
      0)
  }

  /**
   * Groups the tables in the given list of jobs by their owners.
   *
   * @param jobs
   *   a list of Job objects to be grouped
   * @return
   *   a map of owner names to sets of table names owned by each owner
   */
  def jobsGroupByOwners(jobs: List[Job]): Map[String, Set[String]] = {
    jobs
      .flatMap(job => job.tblOwners.map(owner => (owner, getFullyQualifiedTableName(job))))
      .groupBy(_._1)
      .mapValues(_.map(_._2).toSet)
  }

  /**
   * Creates a new mutable.HashMap with owner names from ownerToTablesMap as keys and Boolean as
   * values. The initial value for all the keys will be false.
   *
   * @param ownerToTablesMap
   *   the map with owner names as keys and corresponding table names as values
   * @return
   *   a mutable.HashMap with owner names as keys and false as initial values
   */
  def createBooleanMap(
      ownerToTablesMap: Map[String, Set[String]]): mutable.HashMap[String, Boolean] = {
    val booleanMap = mutable.HashMap.empty[String, Boolean]
    for (owner <- ownerToTablesMap.keys) {
      booleanMap(owner) = false
    }
    booleanMap
  }

  /**
   * Check if Hive table is empty or not
   *
   * @param job
   *   Job object containing information about the table
   */
  def isHiveTableEmpty(job: Job): Boolean = {
    def fetchDataSize: Long =
      metacatService.getTableDataSize(job.catalogName, job.dbName, job.tblName)

    def isEmptyWithSpark: Boolean = {
      val result =
        spark.sql(s"SELECT 1 FROM ${job.catalogName}.${job.dbName}.${job.tblName} LIMIT 1")
      result.isEmpty || result.head.getAs[Int](0) == 0
    }

    if (isHiveTable(job.catalogName, job.dbName, job.tblName)) {
      fetchDataSize match {
        case 0L => true
        case -1L => isEmptyWithSpark
        case _ => false
      }
    } else {
      false
    }
  }

  /**
   * Return future day, date N days from now
   *
   * @param N
   *   Number of days from now
   */
  def getDayNDaysFromNow(N: Int): String = {
    val today = LocalDate.now()
    val futureDate = today.plusDays(N)
    val formatter = DateTimeFormatter.ofPattern("EEEE, MMMM d, yyyy")
    futureDate.format(formatter)
  }

  def consistentHash(value: Int, numBuckets: Int): Int = {
    val md = MessageDigest.getInstance("MD5")
    val hashBytes = md.digest(value.toString.getBytes("UTF-8"))
    val hashString = hashBytes.map("%02x".format(_)).mkString
    val bucket = BigInt(hashString, 16) % numBuckets
    bucket.toInt + 1
  }

  /**
   * Revert a migration finalized table back to Hive table format using shadow tool.
   *
   * @param table
   * The table to be reverted
   * @return
   * Return revert status: true or false
   */
  def revertHelper(job: Job): Unit = {
    val (catalogName, dbName, tableName) = (job.catalogName, job.dbName, job.tblName)
    var archivedHiveTableName = tableName + HIVE_SUFFIX + ARCHIVED_SUFFIX
    if (metacatService.tableExists(catalogName, dbName, archivedHiveTableName)) {
      metacatService.removeTableTags(
        catalogName,
        dbName,
        archivedHiveTableName,
        Set(DO_NOT_DROP_TAG, DO_NOT_RENAME_TAG))
      metacatService.renameTable(catalogName, dbName, archivedHiveTableName, tableName + HIVE_SUFFIX)
    }
  }

  /**
   * Revert a migration finalized table back to Hive table format.
   *
   * @param table
   *   The table to be reverted
   * @return
   *   Return revert status: true or false
   */
  def revertFinalizedTable(table: (String, String, String)): Boolean = {
    val (catalogName, dbName, tableName) = table
    val archivedHiveTableName = tableName + HIVE_SUFFIX + ARCHIVED_SUFFIX
    val archivedIcebergTableName = tableName + ICEBERG_SUFFIX + ARCHIVED_SUFFIX
    logger.info(
      s"Starting revert from $catalogName.$dbName.$archivedHiveTableName to " +
        s"$catalogName.$dbName.$tableName")
    val isIceberg = !isHiveTable(catalogName, dbName, tableName)
    if (isIceberg && metacatService.tableExists(catalogName, dbName, archivedHiveTableName)) {
      val preExistingTags = metacatService.getTableTags(catalogName, dbName, tableName)
      metacatService.removeTableTags(
        catalogName,
        dbName,
        archivedHiveTableName,
        Set(DO_NOT_DROP_TAG, DO_NOT_RENAME_TAG))
      if (metacatService.tableExists(catalogName, dbName, archivedIcebergTableName)) {
        metacatService.forceDropTable(catalogName, dbName, archivedIcebergTableName)
      }
      metacatService.renameTable(catalogName, dbName, tableName, archivedIcebergTableName)
      metacatService.renameTable(catalogName, dbName, archivedHiveTableName, tableName)
      metacatService.appendTableTags(catalogName, dbName, tableName, preExistingTags.toSet)
      logger.info(s"Successfully reverted table $catalogName.$dbName.$tableName")
      true
    } else {
      logger.warn(s"Revert failed for table $catalogName.$dbName.$tableName. ")
      if (!isIceberg) {
        logger.warn("Table is already in Hive format.")
      } else {
        logger.warn(
          "Table migration has not yet finalized." +
            " Wait for table migration to be finalized and then retry.")
      }
      false
    }
  }
}
