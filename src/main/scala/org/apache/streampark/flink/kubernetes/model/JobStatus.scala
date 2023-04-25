package org.apache.streampark.flink.kubernetes.model

import org.apache.streampark.flink.kubernetes.FlinkRestRequest.{JobOverviewInfo, TaskStats}
import org.apache.flink.kubernetes.operator.api.status.JobStatus as FlinkJobStatus

import scala.util.Try

/**
 * see: [[org.apache.streampark.flink.kubernetes.model.JobStatusCV]]
 */
case class JobStatus(
    jobId: String,
    jobName: String,
    state: JobState,
    startTs: Long,
    endTs: Option[Long] = None,
    tasks: Option[TaskStats] = None,
    updatedTs: Long)

enum JobState:
  case INITIALIZING, CREATED, RUNNING, FAILING, FAILED, CANCELLING, CANCELED, FINISHED, RESTARTING, SUSPENDED, RECONCILING
  case UNKNOWN

object JobStatus {
  
  def fromRest(ov: JobOverviewInfo): JobStatus = JobStatus(
    jobId = ov.jid,
    jobName = ov.name,
    state = Try(JobState.valueOf(ov.state)).getOrElse(JobState.UNKNOWN),
    startTs = ov.startTime,
    endTs = Some(ov.endTime),
    updatedTs = ov.lastModifyTime,
    tasks = Some(ov.tasks)
  )

  def fromCR(status: FlinkJobStatus): JobStatus = JobStatus(
    jobId = status.getJobId,
    jobName = status.getJobName,
    state = Try(JobState.valueOf(status.getState)).getOrElse(JobState.UNKNOWN),
    startTs = Try(status.getStartTime.toLong).getOrElse(0L),
    endTs = None,
    updatedTs = Try(status.getUpdateTime.toLong).getOrElse(0L),
    tasks = None
  )
}
