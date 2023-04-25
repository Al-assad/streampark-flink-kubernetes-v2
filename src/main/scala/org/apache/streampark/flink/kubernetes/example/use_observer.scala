package org.apache.streampark.flink.kubernetes.example

import org.apache.streampark.flink.kubernetes.model.TrackKey
import org.apache.streampark.flink.kubernetes.observer.FlinkK8sObserver
import org.apache.streampark.flink.kubernetes.util.{runIO, unsafeRun}
import org.apache.streampark.flink.kubernetes.util.ZStreamExtension
import org.apache.streampark.flink.kubernetes.util.PrettyStringExtension
import org.apache.streampark.flink.kubernetes.util.ConcurrentMapExtension
import org.apache.streampark.flink.kubernetes.util.RefMapExtension
import zio.durationInt
import zio.{Console, ZIO}

/**
 * Track and get flink job snapshot.
 */
@main def getJobSnapshot = unsafeRun {
  for {
    // track resource
    _       <- ZIO.unit
    trackId  = TrackKey.appJob(233, "fdev", "simple-appjob")
    _       <- FlinkK8sObserver.track(trackId)
    // get job snapshot
    _       <- ZIO.sleep(3.seconds)
    jobSnap <- FlinkK8sObserver.evaluatedJobSnaps.getValue(trackId.id)
    _       <- Console.printLine(jobSnap.prettyStr)
  } yield ()
}

/**
 * Track and get flink cluster metrics
 */
@main def getClusterMetric = unsafeRun {
  for {
    // track resource
    _       <- ZIO.unit
    trackId  = TrackKey.appJob(233, "fdev", "simple-appjob")
    _       <- FlinkK8sObserver.track(trackId)
    // get job snapshot
    _       <- ZIO.sleep(3.seconds)
    jobSnap <- FlinkK8sObserver.clusterMetricsSnaps.get((trackId.namespace, trackId.name))
    _       <- Console.printLine(jobSnap.prettyStr)
  } yield ()
}

/**
 * Subscribe Flink job snapshots changes.
 */
@main def subscribeJobSnapshot = unsafeRun {
  for
    // track resource
    _          <- FlinkK8sObserver.track(TrackKey.sessionJob(233, "fdev", "simple-sessionjob", "simple-session"))
    _          <- FlinkK8sObserver.track(TrackKey.appJob(233, "fdev", "simple-appjob"))
    // subscribe job status changes
    watchStream = FlinkK8sObserver.evaluatedJobSnaps.flatSubscribeValues()
    _          <- watchStream.debugPretty.runDrain
  yield ()
}

/**
 * Subscribe Flink cluster metrics changes.
 */
@main def subscribeClusterMetric = unsafeRun {
  for
    // track resource
    _          <- FlinkK8sObserver.track(TrackKey.sessionJob(233, "fdev", "simple-sessionjob", "simple-session"))
    _          <- FlinkK8sObserver.track(TrackKey.appJob(233, "fdev", "simple-appjob"))
    // subscribe job status changes
    watchStream = FlinkK8sObserver.clusterMetricsSnaps.flatSubscribeValues()
    _          <- watchStream.debugPretty.runDrain
  yield ()
}
