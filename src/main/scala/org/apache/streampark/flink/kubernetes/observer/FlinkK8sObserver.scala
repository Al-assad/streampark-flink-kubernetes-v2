package org.apache.streampark.flink.kubernetes.observer

import com.typesafe.scalalogging.Logger
import io.fabric8.kubernetes.client.dsl.Resource
import io.fabric8.kubernetes.client.{KubernetesClient, Watch, Watcher, WatcherException}
import org.apache.flink.kubernetes.operator.api.FlinkDeployment
import org.apache.flink.kubernetes.operator.api.lifecycle.ResourceLifecycleState
import org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus
import org.apache.streampark.flink.kubernetes.*
import org.apache.streampark.flink.kubernetes.K8sTools.watchK8sResource
import org.apache.streampark.flink.kubernetes.model.*
import org.apache.streampark.flink.kubernetes.model.TrackKey.*
import org.apache.streampark.flink.kubernetes.observer.RestSvcEndpointObserver
import zio.ZIO.{attempt, sleep}
import zio.concurrent.{ConcurrentMap, ConcurrentSet}
import zio.stream.{UStream, ZStream}
import zio.{durationInt, Fiber, IO, Queue, Ref, RIO, Schedule, Scope, UIO, URIO, ZIO}
import org.apache.streampark.flink.kubernetes.model.JobStatus

import scala.concurrent.duration.Duration

/**
 * Flink Kubernetes resource observer.
 */
trait FlinkK8sObserver {

  /**
   * Start tracking resources.
   */
  def track(key: TrackKey): IO[Throwable, Unit]

  /**
   * Stop tracking resources.
   */
  def untrack(key: TrackKey): UIO[Unit]

}

type Namespace = String
type Name      = String
type AppId     = Long

object FlinkK8sObserver extends FlinkK8sObserver {

  private val evalJobSnapParallelism = 5
  private val evalJobSnapInterval    = 1.seconds

  val trackedKeys       = ConcurrentSet.empty[TrackKey].runNow
  val evaluatedJobSnaps = Ref.make(Map.empty[AppId, JobSnapshot]).runNow

  val restSvcEndpointSnaps  = ConcurrentMap.empty[(Namespace, Name), RestSvcEndpoint].runNow
  val deployCRSnaps         = ConcurrentMap.empty[(Namespace, Name), (DeployCRStatus, Option[JobStatus])].runNow
  val sessionJobCRSnaps     = ConcurrentMap.empty[(Namespace, Name), (SessionJobCRStatus, Option[JobStatus])].runNow
  val clusterJobStatusSnaps = ConcurrentMap.empty[(Namespace, Name), Vector[JobStatus]].runNow
  val clusterMetricsSnaps   = ConcurrentMap.empty[(Namespace, Name), ClusterMetrics].runNow

  private val restSvcEndpointObserver = RestSvcEndpointObserver(restSvcEndpointSnaps)
  private val deployCrObserver        = DeployCRObserver(deployCRSnaps)
  private val sessionJobCRObserver    = SessionJobCRObserver(sessionJobCRSnaps)
  private val clusterObserver         = RawClusterObserver(restSvcEndpointSnaps, clusterJobStatusSnaps, clusterMetricsSnaps)

  // Auto eval job snapshots forever.
  evalJobSnapshot
    .repeat(Schedule.spaced(evalJobSnapInterval))
    .forever
    .forkDaemon
    .run


  /**
   * Start tracking resources.
   */
  def track(key: TrackKey): IO[Throwable, Unit] = {

    def trackCluster(ns: String, name: String) = {
      deployCrObserver.watch(ns, name) *>
      restSvcEndpointObserver.watch(ns, name) *>
      clusterObserver.watch(ns, name)
    }

    def trackSessionJob(ns: String, name: String, refDeployName: String) = {
      sessionJobCRObserver.watch(ns, name) *>
      trackCluster(ns, refDeployName)
    }

    key match {
      case ApplicationJobKey(id, ns, name)                       => trackCluster(ns, name)
      case SessionJobKey(id, ns, name, clusterName)              => trackSessionJob(ns, name, clusterName)
      case UnmanagedSessionJobKey(id, clusterNs, clusterId, jid) => trackCluster(clusterNs, clusterId)
      case ClusterKey(id, ns, name)                              => trackCluster(ns, name)
    }
  } *> trackedKeys.add(key).unit

  /**
   * Stop tracking resources.
   */
  def untrack(key: TrackKey): UIO[Unit] = {

    def unTrackCluster(ns: String, name: String) = {
      deployCrObserver.unWatch(ns, name) *>
      restSvcEndpointObserver.unWatch(ns, name) *>
      clusterObserver.unWatch(ns, name)
    }

    def unTrackSessionJob(ns: String, name: String) = {
      sessionJobCRObserver.unWatch(ns, name)
    }

    def unTrackPureCluster(ns: String, name: String) = unTrackCluster(ns, name).whenZIO {
      trackedKeys.toSet
        .map(set =>
          set.find {
            case k: ApplicationJobKey if k.namespace == ns && k.name == name           => true
            case k: SessionJobKey if k.namespace == ns && k.clusterName == name        => true
            case k: UnmanagedSessionJobKey if k.clusterNs == ns && k.clusterId == name => true
            case _                                                                     => false
          })
        .map(_.isEmpty)
    }

    def unTrackUnmanagedSessionJob(clusterNs: String, clusterName: String) = unTrackCluster(clusterNs, clusterName).whenZIO {
      trackedKeys.toSet
        .map(set =>
          set.find {
            case k: ApplicationJobKey if k.namespace == clusterNs && k.name == clusterName    => true
            case k: SessionJobKey if k.namespace == clusterNs && k.clusterName == clusterName => true
            case k: ClusterKey if k.namespace == clusterNs && k.name == clusterName           => true
            case _                                                                            => false
          })
        .map(_.isEmpty)
    }

    key match {
      case ApplicationJobKey(id, ns, name)                         => unTrackCluster(ns, name)
      case SessionJobKey(id, ns, name, clusterName)                => unTrackSessionJob(ns, name)
      case ClusterKey(id, ns, name)                                => unTrackPureCluster(ns, name)
      case UnmanagedSessionJobKey(id, clusterNs, clusterName, jid) => unTrackUnmanagedSessionJob(clusterNs, clusterName)
    }
  } *> trackedKeys.remove(key).unit

  /**
   * Re-evaluate all job status snapshots from caches.
   */
  private def evalJobSnapshot: UIO[Unit] = {

    def mergeJobStatus(crStatus: Option[JobStatus], restStatus: Option[JobStatus]) = (crStatus, restStatus) match {
      case (Some(e), None)        => Some(e)
      case (None, Some(e))        => Some(e)
      case (None, None)           => None
      case (Some(cr), Some(rest)) =>
        Some(
          if rest.updatedTs > cr.updatedTs then rest
          else cr.copy(endTs = rest.endTs, tasks = rest.tasks)
        )
    }

    ZStream
      .fromIterableZIO(trackedKeys.toSet)
      .filter { key => key.isInstanceOf[ApplicationJobKey] || key.isInstanceOf[SessionJobKey] || key.isInstanceOf[UnmanagedSessionJobKey] }
      // Evaluate job snapshots for each TrackKey in parallel.
      .mapZIOParUnordered(evalJobSnapParallelism) {
        case ApplicationJobKey(id, ns, name) =>
          for {
            crSnap           <- deployCRSnaps.get(ns, name)
            restJobStatusVec <- clusterJobStatusSnaps.get((ns, name))
            crStatus          = crSnap.map(_._1)

            jobStatusFromCr   = crSnap.flatMap(_._2)
            jobStatusFromRest = restJobStatusVec.flatMap(_.headOption)
            finalJobStatus    = mergeJobStatus(jobStatusFromCr, jobStatusFromRest)

          } yield JobSnapshot(id, ns, name, crStatus, finalJobStatus)

        case SessionJobKey(id, ns, name, clusterName) =>
          for {
            sessionJobSnap   <- sessionJobCRSnaps.get(ns, name)
            restJobStatusVec <- clusterJobStatusSnaps.get((ns, clusterName))
            crStatus          = sessionJobSnap.map(_._1)

            jobStatusFromCr   = sessionJobSnap.flatMap(_._2)
            jobId             = jobStatusFromCr.map(_.jobId).getOrElse("")
            jobStatusFromRest = restJobStatusVec.flatMap(_.find(_.jobId == jobId))
            finalJobStatus    = mergeJobStatus(jobStatusFromCr, jobStatusFromRest)

          } yield JobSnapshot(id, ns, clusterName, crStatus, finalJobStatus)

        case UnmanagedSessionJobKey(id, clusterNs, clusterName, jid) =>
          for {
            restJobStatusVec <- clusterJobStatusSnaps.get((clusterNs, clusterName))
            jobStatus         = restJobStatusVec.flatMap(_.find(_.jobId == jid))
          } yield JobSnapshot(id, clusterNs, clusterName, None, jobStatus)
      }
      // Collect result and Refresh evaluatedJobSnaps cache
      .runCollect
      .map(chunk => chunk.map(snap => (snap.appId, snap)).toMap)
      .flatMap(map => evaluatedJobSnaps.set(map))
      .unit
  }

}
