package com.netflix.migration.utils

import com.netflix.migration.data.{Job, MigrationConfig}
import com.netflix.migration.providers.MigrationConfigProvider
import com.netflix.migration.services.StorageServiceImpl

object StorageUtils {

  val conf: MigrationConfig = MigrationConfigProvider.getMigrationConfig
  val storageService: StorageServiceImpl = StorageServiceImpl(conf)

  def getAllJobs(): List[Job] = {
    val jobsQuery =
      s"""
         |SELECT *
         |FROM ${conf.queueName}
         |""".stripMargin
    storageService.getJobs(jobsQuery)
  }
}
