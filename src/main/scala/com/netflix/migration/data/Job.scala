package com.netflix.migration.data

/**
 * Represents a job entity for migrating a table.
 *
 * @param id
 * @param catalogName
 * @param dbName
 * @param tblName
 * @param stgFormat
 * @param tblOwners
 * @param downstreamUsers
 * @param toBeProcessed
 * @param inProcess
 * @param state
 * @param desiredState
 * @param commLevel1Date
 * @param commLevel2Date
 * @param commLevel3Date
 * @param shadowWatermark
 * @param migrationPaused
 * @param shadowStatus
 * @param createdAt
 * @param lastUpdatedTime
 */
class Job(
    var id: Integer,
    var catalogName: String,
    var dbName: String,
    var tblName: String,
    var stgFormat: String,
    var dataCategory: String,
    var tblOwners: Set[String],
    var downstreamUsers: Set[String],
    var toBeProcessed: Int,
    var inProcess: Int,
    var state: JobState.Value,
    var desiredState: JobState.Value,
    var initialGapInDays: Int,
    var probationGapInDays: Int,
    var commLevel1Date: Long,
    var commLevel2Date: Long,
    var commLevel3Date: Long,
    var shadowWatermark: Long,
    var migrationPaused: Int,
    var pauseReason: String,
    var runId: String,
    var shadowStatus: String,
    var createdAt: Long,
    var lastUpdatedTime: Long) {
  override def toString(): String = {
    s"Job[id=$id, " +
      s"catalog=$catalogName, " +
      s"db=$dbName, " +
      s"tbl=$tblName, " +
      s"format=$stgFormat, " +
      s"data_category=$dataCategory, " +
      s"owners=$tblOwners, " +
      s"to_be_processed=$toBeProcessed, " +
      s"in-process=$inProcess, " +
      s"state=$state, " +
      s"desired=$desiredState, " +
      s"initial_gap_in_days=$initialGapInDays, " +
      s"probation_gap_in_days=$probationGapInDays, " +
      s"comm_level1=$commLevel1Date, " +
      s"comm_level2=$commLevel2Date, " +
      s"comm_level3=$commLevel3Date, " +
      s"shadow_watermark=$shadowWatermark, " +
      s"migration_paused=$migrationPaused, " +
      s"pause_reason=$pauseReason, " +
      s"runId=$runId, " +
      s"shadow_status=$shadowStatus]"
  }
}
