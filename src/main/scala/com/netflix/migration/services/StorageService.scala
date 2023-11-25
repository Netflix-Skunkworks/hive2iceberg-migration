package com.netflix.migration.services

import com.netflix.migration.data.Job

/**
 * Methods interacting with the Jobs database
 */
trait StorageService {

  /**
   * Get the job for the given table
   *
   * @param catalogName
   *   Name of the catalog
   * @param dbName
   *   Name of the database
   * @param tableName
   *   Name of the table
   * @return
   *   Job corresponding to table
   */
  def getJob(catalogName: String, dbName: String, tableName: String): Job

  /**
   * Get the list of jobs for the given query
   *
   * @param jobsQuery
   *   Jobs query
   * @return
   *   list of jobs
   */
  def getJobs(jobsQuery: String): List[Job]

  /**
   * Update the given job's column value.
   *
   * @param job
   *   The job.
   */
  def updateJob(job: Job): Unit

  /**
   * Insert new job into the jobs queue
   *
   * @param job
   * The job.
   */
  def enqueueJob(job: Job): Unit

  /**
   * Remove job from the jobs queue
   *
   * @param job
   * The job.
   */
  def removeJob(job: Job): Unit
}
