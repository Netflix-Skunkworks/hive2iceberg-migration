package com.netflix.migration.providers

import com.netflix.migration.services.MetacatService

object MetacatServiceProvider {
  private var service: MetacatService = _

  /**
   * Initialize the metacat service.
   *
   * @param metacatService the metacat service instance.
   */
  def init(metacatService: MetacatService): Unit = service = metacatService

  def getMetacatService: MetacatService = service
}
