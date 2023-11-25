package com.netflix.migration.services

import com.netflix.migration.utils.MigrationConsts.DataCategory.DataCategory
import com.netflix.ndc.common.dto.{MetadataDto, MetadataResponseDto}

/**
 * An interface for interacting with NDC.
 */
trait NdcService {

  /**
   * Get table metadata
   *
   * @param catalogName
   * @param dbName
   * @param tableName
   */
  def getMetadata(catalogName: String, dbName: String, tableName: String): MetadataResponseDto

  /**
   * Sets table metadata
   *
   * @param metadataDtos
   */
  def setMetadata(metadataDtos: List[MetadataDto]): Unit

  /**
   * Get table owners
   *
   * @param catalogName
   * @param dbName
   * @param tableName
   */
  def getTableOwners(catalogName: String, dbName: String, tableName: String): List[String]

  /**
   * Create MetadataDto object from MetadataResponseDto
   *
   * @param metadataResponseDto
   */
  def createMetadataDto(metadataResponseDto: MetadataResponseDto): MetadataDto

  /**
   * Get table data category
   *
   * @param catalogName
   * @param dbName
   * @param tableName
   */
  def getDataCategory(catalogName: String, dbName: String, tableName: String): DataCategory

}
