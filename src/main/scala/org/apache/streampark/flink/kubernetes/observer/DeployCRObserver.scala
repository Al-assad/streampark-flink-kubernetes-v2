package org.apache.streampark.flink.kubernetes.observer

import io.fabric8.kubernetes.client.Watch
import org.apache.flink.kubernetes.operator.api.FlinkDeployment
import org.apache.streampark.flink.kubernetes.K8sTools
import org.apache.streampark.flink.kubernetes.K8sTools.{usingK8sClient, watchK8sResource, watchK8sResourceForever, K8sResourceWatcher}
import org.apache.streampark.flink.kubernetes.model.{DeployCRStatus, JobSnapshot, JobStatus}
import org.apache.streampark.flink.kubernetes.util.runUIO
import zio.{Fiber, IO, UIO, ZIO}
import zio.concurrent.ConcurrentMap

class DeployCRObserver(
    deployCRSnaps: ConcurrentMap[(Namespace, Name), (DeployCRStatus, Option[JobStatus])]) {

  private val watchers = ConcurrentMap.empty[(Namespace, Name), K8sResourceWatcher[FlinkDeployment]].runUIO

  /**
   * Monitor the status of K8s FlinkDeployment CR for a specified namespace and name.
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

//  private def existCr(namespace: String, name: String): IO[Throwable, Boolean] =
//    usingK8sClient { client =>
//      client
//        .resources(classOf[FlinkDeployment])
//        .inNamespace(namespace)
//        .withName(name)
//        .get != null
//    }

  private def launchProc(namespace: String, name: String): K8sResourceWatcher[FlinkDeployment] =
    watchK8sResourceForever { client =>
      client
        .resources(classOf[FlinkDeployment])
        .inNamespace(namespace)
        .withName(name)
    } { stream =>
      stream
        // Eval FlinkDeployment status
        .map((action, deployment) => DeployCRStatus.eval(action, deployment) -> Option(deployment.getStatus.getJobStatus).map(JobStatus.fromCR))
        // Update FlinkDeployment status cache
        .tap(status => deployCRSnaps.put((namespace, name), status))
    }

}
