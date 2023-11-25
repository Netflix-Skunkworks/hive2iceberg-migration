package com.netflix.migration.providers

import com.netflix.migration.services.{IcebergTableService}

object IcebergTableServiceProvider {
  private var service: IcebergTableService = _

  /**
   * Initialize the iceberg service.
   *
   * @param icebergTableService the service instance.
   */
  def init(icebergTableService: IcebergTableService): Unit = service = icebergTableService

  def getIcebergTableService: IcebergTableService = service
}
