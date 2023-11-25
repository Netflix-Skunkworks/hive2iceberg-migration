package com.netflix.migration.services


/**
 * An interface for interacting with iceberg table metadata.
 */
trait IcebergTableService {

  /**
   * Set the necessary grants for this table.
   *
   * @param catalogName The catalog name.
   * @param dbName The db name.
   * @param tableName The table name.
   * @param grantor name of entity with existing grant permissions on the table.
   */
  def setGrants(catalogName: String, dbName: String, tableName: String, grantor: String)
}
