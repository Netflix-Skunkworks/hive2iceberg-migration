package com.netflix.migration.data

import com.netflix.migration.providers.MigrationConfigProvider
import com.netflix.migration.utils.Utils.getDayNDaysFromNow

case class EmailStrings(initialGapInDays: Int, probationGapInDays: Int) {

  val migrationConf: MigrationConfig = MigrationConfigProvider.getMigrationConfig

  val level1Str1: String =
    if (initialGapInDays > 1) s"${initialGapInDays} Days"
    else if (initialGapInDays == 1) s"${initialGapInDays} Day"
    else "Shortly"
  val level1Str2: String =
    if (initialGapInDays > 0) s"in ${level1Str1.toLowerCase}"
    else s"${level1Str1.toLowerCase}"
  val level1Str3: String =
    if (initialGapInDays > 0) s"within the next ${level1Str1.toLowerCase}" else ""
  val migrationDate = getDayNDaysFromNow(initialGapInDays)
  val lowerLevel1Str1 = level1Str1.toLowerCase
  val level1DownstreamUserMsg: (String, String) = (
    s"[Hive To Iceberg Migration]: Table %s Scheduled for Migration to Iceberg on $migrationDate",
    "Dear User, \n\nWe are writing to inform you that we will be migrating the Hive table\n '%s' \n[https://somelink/tables/%s] " +
      s"to the Iceberg table \nformat on $migrationDate, which is $lowerLevel1Str1 from now. Our records " +
      s"indicate \nthat you have used this table in the past, so we wanted to provide ample notice." +
      s"\n\nUpon completion of the migration, the table owner/s (%s) will have the \n" +
      s"option to restrict access. By default, the table will remain accessible to everyone " +
      s"\n('Common Access')." +
      s"\n\nWe do not anticipate any significant differences or disruptions during the migration " +
      s"\nprocess. For more information about the migration, please visit our migration portal at " +
      s"\n[http://go/h2i-docs]. \n\nIf you have any concerns, questions, or need to pause the migration " +
      s"\nbefore $migrationDate, please reach out to #bigdatasupport Slack channel, and \nwe will be glad to assist you." +
      s"\n\nThanks,\nData Platform")

  val level1TableOwnerMsg: (String, String) = (
    s"[Hive To Iceberg Migration]: Tables Owned by You are Scheduled for Migration to Iceberg on $migrationDate",
    "This email serves as a notification to the owner of following Hive tables %s that they will " +
      s"be migrated as part of the Hive to Iceberg migration process on $migrationDate, \nwhich is " +
      s"$lowerLevel1Str1 from now.\n\n Note that the migrated tables will be granted '" +
      s"""<a href="https://somedocs/">Some docs</a>' """ +
      s"by default. However, as \nthe table owner, you will have the ability to restrict ACLs on the " +
      s"migrated table.\n\n Note that we are currently NOT migrating tables if they use following " +
      s"features: \n* Tables read or written using Metaflow / Fast Data Access. \n* Tables written to " +
      s"using Kragle. \n* Tables using Psycho pattern. \n* Tables using parallel writer WAP pattern. \nIf " +
      s"your table uses any of the above listed features or if there are any other anticipated issues with " +
      s"this \nmigration then please contact the #bigdatasupport Slack channel before $migrationDate to " +
      s"request a \npause and deferral of the migration.")

  val level2Str1: String =
    if (probationGapInDays > 0) s"for ${probationGapInDays} Days"
    else ""
  val level2Str2: String =
    if (probationGapInDays > 0)
      s"within the next ${probationGapInDays} Days"
    else ""
  val level2: (String, String) = (
    s"[Hive To Iceberg Migration]: Table %s on Probation Period $level2Str1",
    "This email is to inform you that the Hive table %s has been migrated to " +
      "the Iceberg format \nas part of the Hive to Iceberg migration process. \n\n If users encounter any " +
      s"issues with the newly-created Iceberg table $level2Str2, please refer to " +
      "https://go/h2i-docs \nfor instructions on how to revert back to the Hive table and pause the migration or " +
      "reach out to #bigdatasupport Slack channel.")

  val level3: (String, String) = (
    "[Hive To Iceberg Migration]: Migration of Table %s from Hive to Iceberg Table Format Complete",
    "This email serves as a notification to the owner or downstream user of " +
      "Hive table %s \nthat it has been successfully migrated to the Iceberg format as part of the " +
      "Hive to Iceberg migration process. \n\nNo further action is required from the table users at this point.")
}
