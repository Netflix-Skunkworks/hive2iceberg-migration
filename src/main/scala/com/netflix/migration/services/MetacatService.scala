package com.netflix.migration.services

import com.netflix.metacat.common.dto.TableDto

import scala.collection.mutable

/**
 * An interface for interacting with Metacat.
 */
trait MetacatService {

  /**
   * Renames the given table to the new table name.
   *
   * @param catalogName
   *   The catalog name.
   * @param dbName
   *   The database name.
   * @param tableName
   *   The table name.
   * @param newTableName
   *   The new table name.
   */
  def renameTable(
      catalogName: String,
      dbName: String,
      tableName: String,
      newTableName: String): Unit

  /**
   * Gets tags on the table
   *
   * @param catalogName
   * @param dbName
   * @param tableName
   */
  def getTableTags(catalogName: String, dbName: String, tableName: String): mutable.Set[String]

  /**
   * Sets tags on the table
   *
   * @param catalogName
   * @param dbName
   * @param tableName
   * @param tag
   */
  def setTableTag(catalogName: String, dbName: String, tableName: String, tag: String): Unit

  /**
   * Append the given tags to the set of existing tags on the table.
   *
   * @param catalogName The catalog name.
   * @param dbName The db name.
   * @param tableName The table name.
   * @param tag The set of tags.
   */
  def appendTableTags(catalogName: String, dbName: String, tableName: String, tags: Set[String]): Unit

  /**
   * Sets and replaces the tags on the table
   *
   * @param catalogName
   * @param dbName
   * @param tableName
   * @param tag
   */
  def setTableTags(catalogName: String, dbName: String, tableName: String, tag: Set[String]): Unit

  /**
   * Sets and replaces the tags on the table if they existed before
   *
   * @param catalogName
   * @param dbName
   * @param tableName
   * @param tag
   */
  def setTableTagsIfExists(
      catalogName: String,
      dbName: String,
      tableName: String,
      preExistingTags: mutable.Set[String],
      tag: Set[String]): Unit

  /**
   * Remove tags from the table
   *
   * @param catalogName
   * @param dbName
   * @param tableName
   * @param tag
   */
  def removeTableTag(catalogName: String, dbName: String, tableName: String, tag: String): Unit

  /**
   * Remove tags from the table
   *
   * @param catalogName
   * @param dbName
   * @param tableName
   * @param tag
   */
  def removeTableTags(
      catalogName: String,
      dbName: String,
      tableName: String,
      tag: Set[String]): Unit

  /**
   * Remove tags from the table if they existed before
   *
   * @param catalogName
   * @param dbName
   * @param tableName
   * @param preExistingTags
   * @param tag
   */
  def removeTableTagsIfExists(
      catalogName: String,
      dbName: String,
      tableName: String,
      preExistingTags: mutable.Set[String],
      tag: Set[String]): Unit

  /**
   * Sets a tag on the table that blocks table updates (renames/deletes/updates).
   *
   * @param catalogName
   * @param dbName
   * @param tableName
   */
  def blockTableWrites(catalogName: String, dbName: String, tableName: String): Unit

  /**
   * Removes a tag on the table that unblocks table updates (renames/deletes/updates).
   *
   * @param catalogName
   * @param dbName
   * @param tableName
   */
  def unBlockTableWrites(catalogName: String, dbName: String, tableName: String): Unit

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
  def tableExists(catalogName: String, dbName: String, tableName: String): Boolean

  /**
   * Deletes the given table.
   *
   * @param catalogName
   * @param dbName
   * @param tableName
   */
  def deleteTable(catalogName: String, dbName: String, tableName: String): Unit

  /**
   * Force drop given table.
   *
   * @param catalogName
   * @param dbName
   * @param tableName
   */
  def forceDropTable(catalogName: String, dbName: String, tblName: String): Unit

  /**
   * Update the table with the given Dto.
   *
   * @param catalogName
   * @param databaseName
   * @param tableName
   * @return
   */
  def updateTable(
      catalogName: String,
      databaseName: String,
      tableName: String,
      tableDto: TableDto): Unit

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
  def getTable(
      catalogName: String,
      databaseName: String,
      tableName: String,
      includeInfo: Boolean = true,
      includeDefinitionMetadata: Boolean = true,
      includeDataMetadata: Boolean = true): Option[TableDto]

  /**
   * Get table storage format.
   *
   * @param catalogName
   * @param databaseName
   * @param tableName
   * @return
   */
  def getTableStorageFormat(catalogName: String, databaseName: String, tableName: String): String

  /**
   * Get table partition count.
   *
   * @param catalogName
   * @param databaseName
   * @param tableName
   * @return
   */
  def getPartitionCount(catalogName: String, databaseName: String, tableName: String): Int

  /**
   * Gets table data size
   *
   * @param catalogName
   * @param dbName
   * @param tableName
   */
  def getTableDataSize(catalogName: String, dbName: String, tableName: String): Long

  /**
   * Set auth to strict secure
   *
   * @param catalogName
   * @param dbName
   * @param tableName
   */
  def setStrictSecure(catalogName: String, dbName: String, tableName: String): Unit

  /**
   * Set table owner
   *
   * @param catalogName
   * @param dbName
   * @param tableName
   */
  def setOwner(catalogName: String, dbName: String, tableName: String, owner: String): Unit
}
