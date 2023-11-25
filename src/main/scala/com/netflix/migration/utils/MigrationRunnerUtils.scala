package com.netflix.migration.utils

import com.netflix.metacat.common.dto.TableDto
import com.netflix.metacat.shaded.com.fasterxml.jackson.databind.ObjectMapper
import com.netflix.metacat.shaded.com.fasterxml.jackson.databind.node.ObjectNode
import com.netflix.migration.data.{Job, MigrationConfig}
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

/**
 * Generic Migration Runner utility methods.
 */
object MigrationRunnerUtils extends StrictLogging {

  @annotation.tailrec
  def retry[T](n: Int)(fn: => T): T = {
    Try {
      fn
    } match {
      case Success(x) => x
      case _ if n > 1 =>
        logger.error(
          s"Failed to execute ${getFnName(fn)} on attempt: $n. Retrying after 5 secs..")
        Thread.sleep(5000)
        retry(n - 1)(fn)
      case Failure(e) => throw e
    }
  }

  def getFnName[T](fn: => T): String = {
    fn.getClass.getName.split("\\$").last
  }

  def applyBatchIdFilterOnJobs(
      spark: SparkSession,
      conf: MigrationConfig,
      jobs: List[Job]): List[Job] = {
    logger.info(
      s"Applying batchId: ${conf.batchId} filter on ${jobs.size} using batchId queue: ${conf.batchTableName}")
    // Get the list of tables matching the batch Id
    val tablesToProcess: Set[String] = spark
      .sql(s"""
         |select distinct table_name from ${conf.batchTableName} t
         |where t.batchId = '${conf.batchId.get.toLowerCase}'
         |""".stripMargin)
      .toDF()
      .collect()
      .map(_(0).asInstanceOf[String].toLowerCase)
      .toSet
    jobs.filter(j =>
      tablesToProcess.contains(
        StringUtils.getTableQualifiedName(j.catalogName, j.dbName, j.tblName).toLowerCase))
  }

  def getMetacatFieldValue(dto: TableDto, fieldName: String): Option[String] = {
    Option(dto.getDefinitionMetadata)
      .flatMap(o => Option(o.get(fieldName)))
      .map(_.asText)
  }

  def setMetacatFieldValue(dto: TableDto, fieldName: String, fieldValue: String) = {
    var definitionMetadataNode: ObjectNode = null
    if (dto.getDefinitionMetadata == null) {
      val objectMapper = new ObjectMapper()
      definitionMetadataNode = objectMapper.createObjectNode()
      dto.setDefinitionMetadata(definitionMetadataNode)
    } else {
      definitionMetadataNode = dto.getDefinitionMetadata
    }
    definitionMetadataNode.put(fieldName, fieldValue)
  }
}
