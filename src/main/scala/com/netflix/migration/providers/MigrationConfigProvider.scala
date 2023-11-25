package com.netflix.migration.providers

import com.netflix.migration.data.MigrationConfig

object MigrationConfigProvider {
  private var conf: MigrationConfig = _

  /**
   * Initialize the migration config.
   *
   * @param migrationConfig
   *   the config to be used for the migration
   */
  def init(migrationConfig: MigrationConfig): Unit = conf = migrationConfig

  def getMigrationConfig: MigrationConfig = conf
}
