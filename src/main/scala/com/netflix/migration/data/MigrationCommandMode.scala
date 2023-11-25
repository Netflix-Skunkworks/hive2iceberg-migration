package com.netflix.migration.data

/**
 * Specifies the command modes for the Migration Runner.
 */
object MigrationCommandMode extends Enumeration {
  type MigrationCommandMode = Value
  val COMMUNICATOR, PREPROCESSOR, MIGRATOR, REVERTER, SHADOWER = Value

  /**
   * Parse the mode string and return the corresponding value
   *
   * @param mode
   *   the mode string to be parsed
   * @return
   *   the corresponding value for the mode string
   */
  def parse(mode: String): Value = {
    values
      .find(_.toString.equalsIgnoreCase(mode))
      .getOrElse(throw new NoSuchElementException(s"No value found for '$mode'"))
  }
}
