package com.netflix.migration.utils

object StringUtils {

  /**
   * Parses String Array with format a=b and returns a Map of key,value pairs
   *
   * @param args
   *   String Array with format a=b
   * @return
   */
  def parseArgs(args: Array[String]): Map[String, String] = {
    var result: Map[String, String] = Map()
    if (args == null) return result

    for (arg <- args) {
      val foo = arg.split("=", 2)
      val value = if (foo.length == 1) "" else foo(1)
      result += (foo(0) -> value)
    }
    result
  }

  def getTableQualifiedName(
      catalogName: String,
      dbName: String,
      tableName: String,
      delim: String = "/"): String = {
    require(
      catalogName != null && dbName != null && tableName != null,
      "Qualified name arguments cannot be null")
    s"$catalogName$delim$dbName$delim$tableName"
  }
}
