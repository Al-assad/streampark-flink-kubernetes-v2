package org.apache.streampark.flink.kubernetes.observer

import org.apache.flink.configuration.{JobManagerOptions, MemorySize}
import org.apache.streampark.flink.kubernetes.FlinkRestRequest
import org.apache.streampark.flink.kubernetes.model.*
import org.apache.streampark.flink.kubernetes.util.runUIO
import zio.concurrent.{ConcurrentMap, ConcurrentSet}
import zio.stream.ZStream
import zio.{durationInt, Chunk, Fiber, Schedule, Scheduler, UIO, ZIO}

import scala.util.Try

class RawClusterObserver(
    restSvcEndpointSnaps: ConcurrentMap[(Namespace, Name), RestSvcEndpoint],
    clusterJobStatusSnaps: ConcurrentMap[(Namespace, Name), Vector[JobStatus]],
    clusterMetricsSnaps: ConcurrentMap[(Namespace, Name), ClusterMetrics]) {

  private val jobOverviewPollFibers    = ConcurrentMap.empty[(Namespace, Name), Fiber.Runtime[_, _]].runUIO
  private val clusterMetricsPollFibers = ConcurrentMap.empty[(Namespace, Name), Fiber.Runtime[_, _]].runUIO

  def watch(namespace: String, name: String): UIO[Unit] = {
    watchJobOverviews(namespace, name) *>
    watchClusterMetrics(namespace, name)
  }

  def unWatch(namespace: String, name: String): UIO[Unit] = {
    unWatchJobOverviews(namespace, name) *>
    unWatchClusterMetrics(namespace, name)
  }

  /**
   * Monitor Flink job overview API.
   */
  // noinspection DuplicatedCode
  private def watchJobOverviews(namespace: String, name: String): UIO[Unit] = {

    val procEf = ZStream
      .fromZIO(
        restSvcEndpointSnaps
          .get(namespace, name)
          .flatMap {
            case None           => ZIO.fail(RestEndpointNotFound)
            case Some(endpoint) => FlinkRestRequest(endpoint.ipRest).listJobOverviewInfo
          })
      .retry(Schedule.spaced(restRetryInterval))
      .repeat(Schedule.spaced(restPollingInterval))
      .map(jobOverviews => jobOverviews.map(info => JobStatus.fromRest(info)))
      .tap(jobStatuses => clusterJobStatusSnaps.put((namespace, name), jobStatuses))
      .runDrain
      .forkDaemon

    jobOverviewPollFibers.get((namespace, name)).flatMap {
      case Some(_) => ZIO.unit
      case None    => procEf.flatMap { fiber => jobOverviewPollFibers.put((namespace, name), fiber) }.unit
    }
  }

  // noinspection DuplicatedCode
  private def unWatchJobOverviews(namespace: String, name: String): UIO[Unit] = {
    jobOverviewPollFibers.get((namespace, name)).flatMap {
      case None        => ZIO.unit
      case Some(fiber) => fiber.interrupt.unit
    }
  }

  /**
   * Monitor Flink cluster metrics via cluster overview API and jm configuration API.
   */
  // noinspection DuplicatedCode
  private def watchClusterMetrics(namespace: String, name: String): UIO[Unit] = {

    val effect = ZStream
      .fromZIO(
        restSvcEndpointSnaps
          .get(namespace, name)
          .flatMap {
            case None           => ZIO.fail(RestEndpointNotFound)
            case Some(endpoint) =>
              FlinkRestRequest(endpoint.ipRest).getClusterOverview <&>
              FlinkRestRequest(endpoint.ipRest).getJobmanagerConfig
          })
      .retry(Schedule.spaced(restRetryInterval))
      .repeat(Schedule.spaced(restPollingInterval))
      .map { (clusterOv, jmConfigs) =>

        val totalJmMemory = Try(MemorySize.parse(jmConfigs.getOrElse("jobmanager.memory.process.size", "0b")).getMebiBytes)
          .getOrElse(0)
        val totalTmMemory = Try(MemorySize.parse(jmConfigs.getOrElse("taskmanager.memory.process.size", "0b")).getMebiBytes * clusterOv.taskManagers)
          .getOrElse(0)

        ClusterMetrics(
          totalJmMemory = totalJmMemory,
          totalTmMemory = totalTmMemory,
          totalTm = clusterOv.taskManagers,
          totalSlot = clusterOv.slotsTotal,
          availableSlot = clusterOv.slotsAvailable,
          runningJob = clusterOv.jobsRunning,
          cancelledJob = clusterOv.jobsFinished,
          failedJob = clusterOv.jobsFailed)
      }
      .tap(metrics => clusterMetricsSnaps.put((namespace, name), metrics))
      .runDrain
      .forkDaemon

    clusterMetricsPollFibers.get((namespace, name)).flatMap {
      case Some(_) => ZIO.unit
      case None    => effect.flatMap { fiber => clusterMetricsPollFibers.put((namespace, name), fiber) }.unit
    }
  }

  // noinspection DuplicatedCode
  private def unWatchClusterMetrics(namespace: String, name: String): UIO[Unit] = {
    clusterMetricsPollFibers.get((namespace, name)).flatMap {
      case None        => ZIO.unit
      case Some(fiber) => fiber.interrupt.unit
    }
  }

  private case object RestEndpointNotFound
}
