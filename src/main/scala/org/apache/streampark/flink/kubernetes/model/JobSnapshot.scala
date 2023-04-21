package org.apache.streampark.flink.kubernetes.model

import io.fabric8.kubernetes.client.{Watch, Watcher}
import org.apache.flink.kubernetes.operator.api.lifecycle.ResourceLifecycleState
import org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus
import org.apache.flink.kubernetes.operator.api.{FlinkDeployment, FlinkSessionJob}
import org.apache.streampark.flink.kubernetes.*

/**
 * Flink job status snapshot identified by StreamPark app-id.
 */
case class JobSnapshot(
    appId: Long,
    clusterNs: String,
    clusterId: String,
    crStatus: Option[FlinkCRStatus],
    jobStatus: Option[JobStatus])


