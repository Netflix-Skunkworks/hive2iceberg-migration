package com.netflix.migration.services

import com.netflix.migration.data.MigrationConfig
import com.netflix.migration.services.TrinoIcebergTableServiceImpl.getTrinoConnection
import com.netflix.migration.utils.{MigrationConsts, StringUtils}
import com.typesafe.scalalogging.StrictLogging

/**
 * Manage Iceberg table metadata using Trino.
 */
case class TrinoIcebergTableServiceImpl(conf: MigrationConfig)
    extends IcebergTableService
    with StrictLogging {

  /**
   * Set the necessary grants for this table.
   *
   * @param catalogName
   *   The catalog name.
   * @param dbName
   *   The db name.
   * @param tableName
   *   The table name.
   * @param grantor
   *   name of entity with existing grant permissions on the table.
   */
  override def setGrants(
      grantor: String,
      catalogName: String,
      dbName: String,
      tableName: String): Unit = {
    // For now, grant all permissions to this GROUP.
    val granteeNames = List("common-access@bdp")
    val tableQName = StringUtils.getTableQualifiedName(catalogName, dbName, tableName)
    var connection: Connection = null
    try {
      connection = getTrinoConnection(grantor)
      // For now, grant all permissions (including ADMIN).
      for (granteeName <- granteeNames) {
        logger.info(
          s"Attempting to grant ADMIN perms to $granteeName using grantor: $grantor on table: $tableQName")
        val grantSqlQuery =
          "GRANT ALL PRIVILEGES ON " + catalogName + "." + dbName + ".\"" + tableName + "\"" + "TO ROLE \"" + granteeName + "\" WITH GRANT OPTION"
        val stmt = connection.createStatement
        stmt.executeUpdate(grantSqlQuery)
        logger.info(
          s"Successfully granted ADMIN perms to $granteeName using grantor: $grantor on table: $tableName")
      }
    } catch {
      case e: Exception =>
        logger.error(s"Encountered exception setting table: $tableName grants. Exception: $e")
        throw e
    } finally {
      if (connection != null) {
        connection.close()
      }
    }
  }
}

object TrinoIcebergTableServiceImpl {

  def getTrinoConnection(user: String): Connection = {
    val trinoJdbcUrl = MigrationConsts.TRINO_JDBC_URL
    val properties = getConnProperties(user)
    DriverManager.registerDriver(new TrinoDriver())
    DriverManager.getConnection(trinoJdbcUrl, properties)
  }

  def getConnProperties(user: String): Properties = {
    val properties = new Properties()
    properties.setProperty("user", user)
    properties.setProperty("SSL", "true")
    properties.setProperty("source", "hive_iceberg_migration")
    properties
  }
}
