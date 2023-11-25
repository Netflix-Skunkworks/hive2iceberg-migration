package com.netflix.migration.modes

/**
 * Methods that each commandMode implements
 */
trait CommandMode {

  /**
   * Run the migration tool in the implemented command mode.
   */
  def run(): Unit
}
