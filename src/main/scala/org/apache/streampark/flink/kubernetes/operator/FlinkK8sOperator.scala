package org.apache.streampark.flink.kubernetes.operator

import org.apache.flink.kubernetes.operator.api.FlinkDeployment
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec
import org.apache.streampark.flink.kubernetes.FlinkRestRequest.{StopJobSptReq, TriggerSptReq}
import org.apache.streampark.flink.kubernetes.hfs.FileMirror
import org.apache.streampark.flink.kubernetes.model.*
import org.apache.streampark.flink.kubernetes.model.TrackKey.{ApplicationJobKey, ClusterKey, SessionJobKey, UnmanagedSessionJobKey}
import org.apache.streampark.flink.kubernetes.observer.FlinkK8sObserver
import org.apache.streampark.flink.kubernetes.util.RefMapExtension
import org.apache.streampark.flink.kubernetes.{FlinkRestRequest, TriggerId}
import zio.ZIO.logErrorCause
import zio.stream.ZStream
import zio.{durationInt, IO, Schedule, ZIO}

trait FlinkK8sOperator {

  /**
   * Directly operate Flink Kubernetes CR.
   */
  val k8sCrOpr: CROperator.type = CROperator

  /**
   * Deploy a Flink cluster with the given spec.
   */
  def deployCluster(id: Long, spec: FlinkDeploymentDef): IO[Throwable, TrackKey.ClusterKey]

  /**
   * Deploy a Flink application job with the given spec.
   */
  def deployApplicationJob(appId: Long, spec: FlinkDeploymentDef): IO[Throwable, TrackKey.ApplicationJobKey]

  /**
   * Deploy a Flink session job with the given spec.
   */
  def deploySessionJob(appId: Long, spec: FlinkSessionJobDef): IO[Throwable, TrackKey.SessionJobKey]

  /**
   * Delete Flink Cluster, Application Job or Session Job resource.
   */
  def delete(id: Long): IO[Throwable, Unit]

  /**
   * Cancel Flink job via rest api.
   */
  def cancelJob(appId: Long): IO[Throwable, Unit]

  /**
   * Stop Flink job with savepoint via rest api.
   */
  def stopJob(appId: Long, savepoint: JobSavepointDef): IO[Throwable, JobSavepointStatus]

  /**
   * Trigger flink job savepoint via rest api.
   */
  def triggerJobSavepoint(appId: Long, savepoint: JobSavepointDef): IO[Throwable, JobSavepointStatus]

}

object FlinkK8sOperator extends FlinkK8sOperator {

  private val obr       = FlinkK8sObserver
  private val flinkRest = FlinkRestRequest

  /**
   * Deploy a Flink cluster with the given spec.
   */
  def deployCluster(id: Long, spec: FlinkDeploymentDef): IO[Throwable, TrackKey.ClusterKey] = {
    for {
      _       <- k8sCrOpr.applyDeployment(spec.copy(job = None))
      trackKey = TrackKey.cluster(id, spec.namespace, spec.name)
      _       <- obr.track(trackKey)
    } yield trackKey
  }

  /**
   * Deploy a Flink application job with the given spec.
   */
  def deployApplicationJob(appId: Long, spec: FlinkDeploymentDef): IO[Throwable, TrackKey.ApplicationJobKey] = {
    for {
      _       <- k8sCrOpr.applyDeployment(spec)
      trackKey = TrackKey.appJob(appId, spec.namespace, spec.name)
      _       <- obr.track(trackKey)
    } yield trackKey
  }

  /**
   * Deploy a Flink session job with the given spec.
   */
  def deploySessionJob(appId: Long, spec: FlinkSessionJobDef): IO[Throwable, TrackKey.SessionJobKey] = {
    for {
      _       <- k8sCrOpr.applySessionJob(spec)
      trackKey = TrackKey.sessionJob(appId, spec.namespace, spec.name, spec.deploymentName)
      _       <- obr.track(trackKey)
    } yield trackKey
  }

  /**
   * Delete Flink Cluster, Application Job or Session Job resource.
   */
  def delete(id: Long): IO[Throwable, Unit] =
    for {
      trackKey <- obr.trackedKeys
                    .find(_.id == id)
                    .someOrFail(FlinkResourceNotFound(id))
      _        <- trackKey match {
                    case ClusterKey(_, namespace, name)        => k8sCrOpr.deleteDeployment(namespace, name)
                    case ApplicationJobKey(_, namespace, name) => k8sCrOpr.deleteDeployment(namespace, name)
                    case SessionJobKey(_, namespace, name, _)  => k8sCrOpr.deleteSessionJob(namespace, name)
                    case _                                     => ZIO.fail(UnsupportedAction(s"delete resource for $trackKey"))
                  }
    } yield ()

  /**
   * Cancel Flink job via rest api.
   */
  def cancelJob(appId: Long): IO[Throwable, Unit] =
    for {
      hook                 <- retrieveFlinkJobEndpoint(appId)
      (restEndpoint, jobId) = hook
      _                    <- flinkRest(restEndpoint.chooseRest).cancelJob(jobId)
    } yield ()

  /**
   * Stop Flink job with savepoint via rest api.
   */
  // noinspection DuplicatedCode
  def stopJob(appId: Long, savepoint: JobSavepointDef): IO[Throwable, JobSavepointStatus] =
    for {
      hook                 <- retrieveFlinkJobEndpoint(appId)
      (restEndpoint, jobId) = hook
      rest                  = flinkRest(restEndpoint.chooseRest)
      // submit stop job request
      triggerId            <- rest.stopJobWithSavepoint(jobId, StopJobSptReq(savepoint))
      // watch trigger status until it's finished
      triggerRs            <- ZStream
                                .fromZIO(rest.getSavepointOperationStatus(jobId, triggerId))
                                .repeat(Schedule.spaced(100.millis))
                                .takeUntil(_.isCompleted)
                                .runLast
                                .map(_.get)
    } yield triggerRs

  /**
   * Trigger flink job savepoint via rest api.
   */
  // noinspection DuplicatedCode
  def triggerJobSavepoint(appId: Long, savepoint: JobSavepointDef): IO[Throwable, JobSavepointStatus] =
    for {
      hook                 <- retrieveFlinkJobEndpoint(appId)
      (restEndpoint, jobId) = hook
      rest                  = flinkRest(restEndpoint.chooseRest)
      // submit stop job request
      triggerId            <- rest.triggerSavepoint(jobId, TriggerSptReq(savepoint))
      // watch trigger status until it's finished
      triggerRs            <- ZStream
                                .fromZIO(rest.getSavepointOperationStatus(jobId, triggerId))
                                .repeat(Schedule.spaced(100.millis))
                                .takeUntil(_.isCompleted)
                                .runLast
                                .map(_.get)
    } yield triggerRs

  private type JobId = String
  private def retrieveFlinkJobEndpoint(appId: Long): IO[Throwable, (RestSvcEndpoint, JobId)] =
    for {
      // Find track key
      trackKey <- obr.trackedKeys.find(_.id == appId).someOrFail(FlinkResourceNotFound(appId))

      // Find ref flink cluster rest endpoint
      restEpt <- obr.restSvcEndpointSnaps
                   .get((trackKey.clusterNamespace, trackKey.clusterName))
                   .someOrFail(FlinkRestEndpointNotFound(trackKey.clusterNamespace, trackKey.clusterName))

      // Find flink job id
      jobId   <- trackKey match {
                   case UnmanagedSessionJobKey(_, _, _, jobId) => ZIO.succeed(jobId)
                   case _                                      =>
                     obr.evaluatedJobSnaps
                       .getValue(trackKey.id)
                       .map(snap => snap.flatMap(_.jobStatus).map(_.jobId))
                       .someOrFail(FlinkJobNotFound(trackKey.id))
                 }
    } yield (restEpt, jobId)
}

case class UnsupportedAction(msg: String) extends Exception("Unsupported action: " + msg)

case class FlinkResourceNotFound(id: Long) extends Exception(s"Flink resource not found: id=${id}")

case class FlinkJobNotFound(appId: Long) extends Exception(s"Flink job not found: appId=${appId}")

case class FlinkRestEndpointNotFound(namespace: String, name: String)
    extends Exception(s"Flink cluster rest endpoint not found: namespace=${namespace}, name=${name}")
