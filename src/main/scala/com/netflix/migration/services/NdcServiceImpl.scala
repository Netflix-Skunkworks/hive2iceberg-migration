package com.netflix.migration.services

import com.netflix.ndc.client.Client
import com.netflix.migration.data.MigrationConfig
import com.netflix.migration.utils.MigrationRunnerUtils.retry
import com.netflix.migration.utils.StringUtils.getTableQualifiedName
import com.netflix.migration.utils.MigrationConsts
import com.netflix.migration.utils.MigrationConsts.DataCategory.{CORE_PI, DataCategory, NO_PI, PI, UNKNOWN}
import com.netflix.ndc.common.QualifiedName
import com.netflix.ndc.common.dto.MetadataDto.MetadataDtoBuilder
import com.netflix.ndc.common.dto.{MetadataDto, MetadataResponseDto}
import com.typesafe.scalalogging.StrictLogging

import scala.collection.JavaConverters._

case class NdcServiceImpl(conf: MigrationConfig) extends NdcService with StrictLogging {
  @transient private[services] lazy val ndcClient = Client
    .builder()
    .withHost(MigrationConsts.NDC_HOST_URL_PROPERTY)
    .withUserName(MigrationConsts.NDC_USERNAME_PROPERTY)
    .withClientAppName(MigrationConsts.NDC_CLIENT_APP_NAME_PROPERTY)
    .build()

  /**
   * Get table metadata
   *
   * @param catalogName
   * @param dbName
   * @param tableName
   */
  def getMetadata(catalogName: String, dbName: String, tableName: String): MetadataResponseDto = {
    val qualifiedName = QualifiedName.fromString(
      String.format("ndc://hive/%s/%s/%s", catalogName, dbName, tableName))
    val metadataDtos = ndcClient.getMetadataV0Api.getMetadata(qualifiedName, true)
    if (!metadataDtos.isEmpty) {
      logger.info(s"Successfully getting NDC metadata for table ${qualifiedName.getFullName}")
      metadataDtos.get(0)
    } else null
  }

  /**
   * Sets table metadata
   *
   * @param metadataDtos
   */
  def setMetadata(metadataDtos: List[MetadataDto]): Unit = {
    retry(MigrationConsts.NUM_RETRIES)(
      ndcClient.getBulkMetadataV0Api.setMetadata(metadataDtos.asJava))
    logger.info(
      s"Successfully setting NDC metadata for table ${metadataDtos.head.getName.getFullName}")
  }

  /**
   * Get table owners
   *
   * @param catalogName
   * @param dbName
   * @param tableName
   */
  def getTableOwners(catalogName: String, dbName: String, tableName: String): List[String] = {
    val tableQualifiedName = getTableQualifiedName(catalogName, dbName, tableName)
    val metadataResponseDto = getMetadata(catalogName, dbName, tableName)
    if (metadataResponseDto != null) {
      metadataResponseDto.getVerifiedTechnicalContacts.asScala.toList
    } else {
      logger.warn(s"Failed to get table owners information for table $tableQualifiedName")
      List.empty[String]
    }
  }

  /**
   * Get table data category
   *
   * @param catalogName
   * @param dbName
   * @param tableName
   */
  def getDataCategory(catalogName: String, dbName: String, tableName: String): DataCategory = {
    val tableQualifiedName = getTableQualifiedName(catalogName, dbName, tableName)
    Option(getMetadata(catalogName, dbName, tableName)) match {
      case Some(metadataResponseDto) =>
        Option(metadataResponseDto.getDlmDataCategoryTags) match {
          case Some(dlmDataCategoryTags) =>
            dlmDataCategoryTags.asScala.get("core_pi") match {
              case Some(_) => CORE_PI
              case None =>
                dlmDataCategoryTags.asScala.get("pi") match {
                  case Some("yes") => PI
                  case Some("no") => NO_PI
                  case _ => UNKNOWN
                }
            }
          case None => UNKNOWN
        }
      case None =>
        logger.warn(s"Failed to get data category information for table $tableQualifiedName")
        UNKNOWN
    }
  }

  /**
   * Create MetadataDto object from MetadataResponseDto
   *
   * @param metadataResponseDto
   */
  def createMetadataDto(metadataResponseDto: MetadataResponseDto): MetadataDto = {
    MetadataDto
      .builder()
      .asInstanceOf[MetadataDtoBuilder[MetadataDto, _]]
      .name(metadataResponseDto.getName)
      .asInstanceOf[MetadataDtoBuilder[MetadataDto, _]]
      .id(metadataResponseDto.getId)
      .asInstanceOf[MetadataDtoBuilder[MetadataDto, _]]
      .audit(metadataResponseDto.getAudit)
      .asInstanceOf[MetadataDtoBuilder[MetadataDto, _]]
      .dataSize(metadataResponseDto.getDataSize)
      .asInstanceOf[MetadataDtoBuilder[MetadataDto, _]]
      .disavowedTechnicalContactPandoraIDs(
        metadataResponseDto.getDisavowedTechnicalContactPandoraIDs)
      .asInstanceOf[MetadataDtoBuilder[MetadataDto, _]]
      .dlmDataCategory(metadataResponseDto.getDlmDataCategory)
      .asInstanceOf[MetadataDtoBuilder[MetadataDto, _]]
      .dlmDataCategoryTags(metadataResponseDto.getDlmDataCategoryTags)
      .asInstanceOf[MetadataDtoBuilder[MetadataDto, _]]
      .dlmMetadata(metadataResponseDto.getDlmMetadata)
      .asInstanceOf[MetadataDtoBuilder[MetadataDto, _]]
      .dlmOwners(metadataResponseDto.getDlmOwners)
      .asInstanceOf[MetadataDtoBuilder[MetadataDto, _]]
      .inferredTechnicalContactPandoraIDs(
        metadataResponseDto.getInferredTechnicalContactPandoraIDs)
      .asInstanceOf[MetadataDtoBuilder[MetadataDto, _]]
      .isTemporary(metadataResponseDto.getIsTemporary)
      .asInstanceOf[MetadataDtoBuilder[MetadataDto, _]]
      .labels(metadataResponseDto.getLabels)
      .asInstanceOf[MetadataDtoBuilder[MetadataDto, _]]
      .lifetime(metadataResponseDto.getLifetime)
      .asInstanceOf[MetadataDtoBuilder[MetadataDto, _]]
      .location(metadataResponseDto.getLocation)
      .asInstanceOf[MetadataDtoBuilder[MetadataDto, _]]
      .metadata(metadataResponseDto.getMetadata)
      .asInstanceOf[MetadataDtoBuilder[MetadataDto, _]]
      .sourceLink(metadataResponseDto.getSourceLink)
      .asInstanceOf[MetadataDtoBuilder[MetadataDto, _]]
      .syncDate(metadataResponseDto.getSyncDate)
      .asInstanceOf[MetadataDtoBuilder[MetadataDto, _]]
      .verifiedTechnicalContactPandoraIDs(
        metadataResponseDto.getVerifiedTechnicalContactPandoraIDs)
      .asInstanceOf[MetadataDtoBuilder[MetadataDto, _]]
      .build
  }
}
