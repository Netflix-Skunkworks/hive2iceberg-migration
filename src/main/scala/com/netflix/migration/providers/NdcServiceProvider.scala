package com.netflix.migration.providers

import com.netflix.migration.services.NdcService

object NdcServiceProvider {
  private var service: NdcService = _

  /**
   * Initialize the ndc service.
   *
   * @param ndcService the ndc service instance.
   */
  def init(ndcService: NdcService): Unit = service = ndcService

  def getNdcService: NdcService = service
}
