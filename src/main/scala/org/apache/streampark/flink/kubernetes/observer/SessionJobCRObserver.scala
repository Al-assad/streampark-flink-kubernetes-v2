package org.apache.streampark.flink.kubernetes.observer

import io.fabric8.kubernetes.api.model.Service
import io.fabric8.kubernetes.client.Watch
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob
import org.apache.streampark.flink.kubernetes.K8sTools.{watchK8sResource, watchK8sResourceForever, K8sResourceWatcher}
import org.apache.streampark.flink.kubernetes.model.{DeployCRStatus, JobStatus, SessionJobCRStatus}
import org.apache.streampark.flink.kubernetes.util.runUIO
import zio.{Fiber, UIO, ZIO}
import zio.concurrent.ConcurrentMap
import org.apache.flink.kubernetes.operator.api.status.JobStatus as FlinkJobStatus

class SessionJobCRObserver(
    sessionJobCRSnaps: ConcurrentMap[(Namespace, Name), (SessionJobCRStatus, Option[JobStatus])]) {

  private val watchers = ConcurrentMap.empty[(Namespace, Name), K8sResourceWatcher[FlinkSessionJob]].runUIO

  /**
   * Monitor the status of Flink SessionJob K8s CR for a specified namespace and name.
   */
  // noinspection DuplicatedCode
  def watch(namespace: String, name: String): UIO[Unit] =
    watchers.get((namespace, name)).flatMap {
      case Some(_) => ZIO.unit
      case None    =>
        val watch = launchProc(namespace, name)
        watchers.put((namespace, name), watch) *>
        watch.launch
    }

  // noinspection DuplicatedCode
  def unWatch(namespace: String, name: String): UIO[Unit] =
    watchers.get((namespace, name)).flatMap {
      case None          => ZIO.unit
      case Some(watcher) => watcher.stop *> watchers.remove((namespace, name)).unit
    }

  private def launchProc(namespace: String, name: String): K8sResourceWatcher[FlinkSessionJob] =
    watchK8sResourceForever { client =>
      client
        .resources(classOf[FlinkSessionJob])
        .inNamespace(namespace)
        .withName(name)
    } { stream =>
      stream
        // Eval SessionJobCR status
        .map((action, cr) => SessionJobCRStatus.eval(action, cr) -> Option(cr.getStatus.getJobStatus).map(JobStatus.fromCR))
        // Update SessionJobCR status cache
        .tap(status => sessionJobCRSnaps.put((namespace, name), status))
    }

}
