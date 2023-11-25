package com.netflix.migration.services

import com.netflix.metacat.client.Client
import com.netflix.metacat.common.dto.TableDto
import com.netflix.metacat.shaded.com.fasterxml.jackson.databind.node.ObjectNode
import com.netflix.migration.data.MigrationConfig
import com.netflix.migration.utils.MigrationConsts.{DEFAULT_TABLE_OWNER, DO_NOT_DROP_TAG, DO_NOT_RENAME_TAG, HIVE_CSV_TEXT, HIVE_PARQUET, ICEBERG_MIGRATION_DO_NOT_MODIFY_TAG}
import com.netflix.migration.utils.MigrationRunnerUtils.retry
import com.netflix.migration.utils.Utils.metacatService
import com.netflix.migration.utils.{MigrationConsts, StringUtils}
import com.typesafe.scalalogging.StrictLogging

import scala.collection.JavaConverters._
import scala.collection.mutable

case class MetacatServiceImpl(conf: MigrationConfig) extends MetacatService with StrictLogging {
  @transient private[services] lazy val metacatClient = Client
    .builder()
    .withHost(MigrationConsts.METACAT_HOST_URL_PROPERTY)
    .withDataTypeContext("hive")
    .withUserName(MigrationConsts.METACAT_USERNAME_PROPERTY)
    .withClientAppName(MigrationConsts.METACAT_CLIENT_APP_NAME_PROPERTY)
    .build()

  override def renameTable(
      catalogName: String,
      dbName: String,
      tableName: String,
      newTableName: String): Unit = {
    val currentTableQName = StringUtils.getTableQualifiedName(catalogName, dbName, tableName)
    val newTableQName = StringUtils.getTableQualifiedName(catalogName, dbName, newTableName)

    try {
      val preExistingTags = getTableTags(catalogName, dbName, tableName)
      removeTableTagsIfExists(
        catalogName,
        dbName,
        tableName,
        preExistingTags,
        Set(MigrationConsts.DO_NOT_RENAME_TAG, MigrationConsts.DO_NOT_DROP_TAG))

      metacatClient.getApi.renameTable(catalogName, dbName, tableName, newTableName)

      setTableTagsIfExists(
        catalogName,
        dbName,
        newTableName,
        preExistingTags,
        Set(MigrationConsts.DO_NOT_RENAME_TAG, MigrationConsts.DO_NOT_DROP_TAG))

      logger.info(s"Successfully renamed table $currentTableQName to $newTableQName")
    } catch {
      case e: Exception =>
        println(s"Failed to rename table $currentTableQName to $newTableQName. Exception: $e")
        throw e
    }
  }

  /**
   * Gets table dto object
   *
   * @param catalogName
   * @param dbName
   * @param tableName
   */
  def getTableDto(catalogName: String, dbName: String, tableName: String): TableDto = {
    val fullTableName = s"$catalogName/$dbName/$tableName"
    val optionalTable = getTable(catalogName, dbName, tableName, includeDataMetadata = false)
    if (optionalTable.isEmpty) {
      println(s"Table: $fullTableName not found in metacat")
      throw new RuntimeException(s"Table $fullTableName not found in metacat!")
    }
    optionalTable.get
  }

  /**
   * Gets tags on the table
   *
   * @param catalogName
   * @param dbName
   * @param tableName
   */
  def getTableTags(
      catalogName: String,
      dbName: String,
      tableName: String): mutable.Set[String] = {
    val tableDto = getTableDto(catalogName, dbName, tableName)

    val tagsSet: mutable.HashSet[String] = mutable.HashSet()
    if (tableDto.getDefinitionMetadata != null) {
      val tagsNode = tableDto.getDefinitionMetadata.path("tags")
      if (tagsNode != null &&
        tagsNode.isArray &&
        tagsNode.size() > 0) {
        // Get the existing set of tags and append to them since
        // the tags API is a replace all tags API.
        for (tag <- tagsNode.asScala) {
          tagsSet.add(tag.asText().trim)
        }
      }
    } else {
      return null
    }
    tagsSet
  }

  /**
   * Sets tags on the table
   *
   * @param catalogName
   * @param dbName
   * @param tableName
   * @param tag
   */
  override def setTableTag(
      catalogName: String,
      dbName: String,
      tableName: String,
      tag: String): Unit = {
    val tableQName = StringUtils.getTableQualifiedName(catalogName, dbName, tableName)
    if (!tableExists(catalogName, dbName, tableName)) {
      logger.error(s"Table: $tableQName does not exist. Skipping tag update.")
      return
    }
    var tagsSet: mutable.Set[String] = getTableTags(catalogName, dbName, tableName)
    if (tagsSet != null) {
      if (!tagsSet.contains(tag)) {
        tagsSet += tag
      }
      setTableTags(catalogName, dbName, tableName, tagsSet.toSet)
      logger.info(s"Successfully setting tag $tag from table $tableQName")
    }
  }

  /**
   * Append the given tags to the set of existing tags on the table.
   *
   * @param catalogName
   *   The catalog name.
   * @param dbName
   *   The db name.
   * @param tableName
   *   The table name.
   * @param tag
   *   The set of tags.
   */
  def appendTableTags(
      catalogName: String,
      dbName: String,
      tableName: String,
      tags: Set[String]): Unit = {
    if (!tableExists(catalogName, dbName, tableName)) {
      logger.error(s"Table: $tableName does not exist. Skipping tag append operation.")
      return
    }
    if (tags.isEmpty) {
      logger.error(s"Tags set to append is empty. Skipping tag append operation.")
      return
    }

    // Get the existing tags and append, deduplication will be handled by ++= operator
    val existingTags = getTableTags(catalogName, dbName, tableName)
    existingTags ++= tags
    setTableTags(catalogName, dbName, tableName, existingTags.toSet)
  }

  /**
   * @param catalogName
   * @param dbName
   * @param tableName
   * @param tag
   */
  def setTableTags(
      catalogName: String,
      dbName: String,
      tableName: String,
      tagsSet: Set[String]): Unit = {
    if (!tableExists(catalogName, dbName, tableName)) {
      logger.error(s"Table: $tableName does not exist. Skipping tag update.")
      return
    }
    retry(MigrationConsts.NUM_RETRIES)(
      metacatClient.getTagApi.setTableTags(catalogName, dbName, tableName, tagsSet.asJava))
  }

  /**
   * Sets and replaces the tags on the table if they existed before
   *
   * @param catalogName
   * @param dbName
   * @param tableName
   * @param tagsSet
   */
  def setTableTagsIfExists(
      catalogName: String,
      dbName: String,
      tableName: String,
      preExistingTags: mutable.Set[String],
      tags: Set[String]): Unit = {
    for (tag <- tags) {
      if (preExistingTags != null && preExistingTags.contains(tag)) {
        retry(MigrationConsts.NUM_RETRIES)(setTableTag(catalogName, dbName, tableName, tag))
      }
    }
  }

  /**
   * Remove tags from the table
   *
   * @param catalogName
   * @param dbName
   * @param tableName
   * @param tag
   */
  def removeTableTag(
      catalogName: String,
      dbName: String,
      tableName: String,
      tag: String): Unit = {
    val tableQName = StringUtils.getTableQualifiedName(catalogName, dbName, tableName)
    metacatClient.getTagApi.removeTableTags(
      catalogName,
      dbName,
      tableName,
      false,
      Set(tag).asJava)
    logger.info(s"Successfully removed tag $tag from table $tableQName")
  }

  /**
   * Remove tags from the table
   *
   * @param catalogName
   * @param dbName
   * @param tableName
   * @param tagsSet
   */
  def removeTableTags(
      catalogName: String,
      dbName: String,
      tableName: String,
      tags: Set[String]): Unit = {
    val preExistingTags = getTableTags(catalogName, dbName, tableName)
    removeTableTagsIfExists(catalogName, dbName, tableName, preExistingTags, tags)
  }

  /**
   * Remove tags from the table if they existed before
   *
   * @param catalogName
   * @param dbName
   * @param tableName
   * @param preExistingTags
   * @param tagsSet
   */
  def removeTableTagsIfExists(
      catalogName: String,
      dbName: String,
      tableName: String,
      preExistingTags: mutable.Set[String],
      tags: Set[String]): Unit = {
    for (tag <- tags) {
      if (preExistingTags != null && preExistingTags.contains(tag)) {
        removeTableTag(catalogName, dbName, tableName, tag)
      }
    }
  }

  /**
   * Sets a tag on the table that blocks table updates (renames/deletes/updates).
   *
   * @param catalogName
   * @param dbName
   * @param tableName
   */
  override def blockTableWrites(catalogName: String, dbName: String, tableName: String): Unit = {
    setTableTag(
      catalogName,
      dbName,
      tableName,
      MigrationConsts.ICEBERG_MIGRATION_DO_NOT_MODIFY_TAG)
  }

  /**
   * Removes a tag on the table that unblocks table updates (renames/deletes/updates).
   *
   * @param catalogName
   * @param dbName
   * @param tableName
   */
  override def unBlockTableWrites(
      catalogName: String,
      dbName: String,
      tableName: String): Unit = {
    val fullTableName = s"$catalogName/$dbName/$tableName"
    if (tableExists(catalogName, dbName, tableName)) {
      logger.info(s"Unblocking writes for table: $fullTableName")
      metacatClient.getTagApi.removeTableTags(
        catalogName,
        dbName,
        tableName,
        false,
        Set(MigrationConsts.ICEBERG_MIGRATION_DO_NOT_MODIFY_TAG).asJava)
      logger.info(s"Successfully unblocked writes to table: $fullTableName")
    }
  }

  /**
   * Get the table from metacat.
   *
   * @param catalogName
   * @param databaseName
   * @param tableName
   * @param includeInfo
   * @param includeDefinitionMetadata
   * @param includeDataMetadata
   * @return
   */
  override def getTable(
      catalogName: String,
      databaseName: String,
      tableName: String,
      includeInfo: Boolean,
      includeDefinitionMetadata: Boolean,
      includeDataMetadata: Boolean): Option[TableDto] = {
    val fullTableName = s"$catalogName/$databaseName/$tableName"

    try {
      val result = metacatClient.getApi
        .getTable(
          catalogName,
          databaseName,
          tableName,
          includeInfo,
          includeDefinitionMetadata,
          includeDataMetadata)
      if (result == null) {
        println(s"Table $fullTableName not found in metacat!")
        None
      } else {
        Some(result)
      }
    } catch {
      case e: Exception =>
        println(s"getTable($fullTableName) from metacat failed. Exception: $e")
        None
    }
  }

  /**
   * Checks if the given table exists.
   *
   * @param catalogName
   *   The catalog name.
   * @param dbName
   *   The database name.
   * @param tableName
   *   The table name.
   * @return
   */
  override def tableExists(catalogName: String, dbName: String, tableName: String): Boolean = {
    val tableQName = StringUtils.getTableQualifiedName(catalogName, dbName, tableName)
    try {
      logger.info(
        s"Checking if table ${StringUtils.getTableQualifiedName(catalogName, dbName, tableName)} exists")
      metacatClient.getApi.doesTableExist(catalogName, dbName, tableName)
    } catch {
      case e: Exception =>
        logger.error(s"Failed to check if table: $tableQName exists in metacat. Exception: $e")
        throw e
    }
  }

  /**
   * Deletes the given table.
   *
   * @param catalogName
   * @param dbName
   * @param tableName
   */
  override def deleteTable(catalogName: String, dbName: String, tableName: String): Unit = {
    val tableQName = StringUtils.getTableQualifiedName(catalogName, dbName, tableName)
    try {
      metacatService.removeTableTags(
        catalogName,
        dbName,
        tableName,
        Set(DO_NOT_RENAME_TAG, DO_NOT_DROP_TAG, ICEBERG_MIGRATION_DO_NOT_MODIFY_TAG))
      metacatClient.getApi.deleteTable(catalogName, dbName, tableName)
      logger.info(s"Successfully deleted table: $tableQName from metacat.")
    } catch {
      case e: Exception =>
        logger.error(s"Failed to delete table: $tableQName from metacat. Exception: $e")
        throw e
    }
  }

  /**
   * Force drop given table.
   *
   * @param catalogName
   * @param dbName
   * @param tableName
   */
  def forceDropTable(catalogName: String, dbName: String, tblName: String): Unit = {
    try {
      metacatService.removeTableTags(
        catalogName,
        dbName,
        tblName,
        Set(DO_NOT_RENAME_TAG, DO_NOT_DROP_TAG, ICEBERG_MIGRATION_DO_NOT_MODIFY_TAG))
      metacatService.deleteTable(catalogName, dbName, tblName)
    } catch {
      case e: Throwable =>
        println(s"Ignoring exception during deleteTable for $tblName")
    }
  }

  /**
   * Update the table with the given Dto.
   *
   * @param catalogName
   * @param databaseName
   * @param tableName
   * @return
   */
  override def updateTable(
      catalogName: String,
      databaseName: String,
      tableName: String,
      tableDto: TableDto): Unit = {
    val tableQName = StringUtils.getTableQualifiedName(catalogName, databaseName, tableName)
    try {
      metacatClient.getApi.updateTable(catalogName, databaseName, tableName, tableDto)
      logger.info(s"Successfully updated table: $tableQName in metacat.")
    } catch {
      case e: Exception =>
        logger.error(s"Failed to update table: $tableQName in metacat. Exception: $e")
        throw e
    }
  }

  /**
   * Get table storage format.
   *
   * @param catalogName
   * @param databaseName
   * @param tableName
   * @return
   */
  override def getTableStorageFormat(
      catalogName: String,
      databaseName: String,
      tableName: String): String = {
    val tableDto = getTableDto(catalogName, databaseName, tableName)
    if (tableDto.getSerde.getOutputFormat.toLowerCase.contains("parquet")) {
      HIVE_PARQUET
    } else {
      HIVE_CSV_TEXT
    }
  }

  /**
   * Get table partition count.
   *
   * @param catalogName
   * @param databaseName
   * @param tableName
   * @return
   */
  def getPartitionCount(catalogName: String, databaseName: String, tableName: String): Int = {
    metacatClient.getPartitionApi.getPartitionCount(catalogName, databaseName, tableName)
  }

  /**
   * Gets table data size
   *
   * @param catalogName
   * @param dbName
   * @param tableName
   */
  def getTableDataSize(catalogName: String, dbName: String, tableName: String): Long = {
    val tableDto = getTableDto(catalogName, dbName, tableName)

    Option(tableDto.getDefinitionMetadata)
      .flatMap { metadata =>
        Option(metadata.get("data_size")).map(_.asLong)
      }
      .getOrElse(-1L)
  }

  /**
   * Set auth to strict secure
   *
   * @param catalogName
   * @param dbName
   * @param tableName
   */
  def setStrictSecure(catalogName: String, dbName: String, tableName: String): Unit = {
    val tableDto = getTableDto(catalogName, dbName, tableName)
    tableDto.getDefinitionMetadata.put("secure", true)
    metacatClient.getApi.updateTable(catalogName, dbName, tableName, tableDto)
  }

  /**
   * Set table owner
   *
   * @param catalogName
   * @param dbName
   * @param tableName
   */
  def setOwner(catalogName: String, dbName: String, tableName: String, owner: String): Unit = {
    val tableDto = getTableDto(catalogName, dbName, tableName)
    val ownerNode = tableDto.getDefinitionMetadata.get("owner").asInstanceOf[ObjectNode]
    ownerNode.put("userId", DEFAULT_TABLE_OWNER)
    metacatClient.getApi.updateTable(catalogName, dbName, tableName, tableDto)
  }
}
