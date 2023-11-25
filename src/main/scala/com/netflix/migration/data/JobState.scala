package com.netflix.migration.data

object JobState extends Enumeration {
  type JobState = Value

  val Ready, WritesBlocked, IcebergReady, IcebergPrimary, WritesUnblocked, HiveDropped,
      HivePrimary, SyncHive, IcebergDropped, Undefined = Value
}
