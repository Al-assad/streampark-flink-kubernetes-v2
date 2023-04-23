package org.apache.streampark.flink.kubernetes.observer

import io.fabric8.kubernetes.client.Watch
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob
import org.apache.streampark.flink.kubernetes.tool.K8sTools.watchK8sResource
import org.apache.streampark.flink.kubernetes.model.{DeployCRStatus, JobStatus, SessionJobCRStatus}
import org.apache.streampark.flink.kubernetes.tool.runUIO
import zio.{Fiber, UIO, ZIO}
import zio.concurrent.ConcurrentMap
import org.apache.flink.kubernetes.operator.api.status.JobStatus as FlinkJobStatus

class SessionJobCRObserver(
    sessionJobCRSnaps: ConcurrentMap[(Namespace, Name), (SessionJobCRStatus, Option[JobStatus])]) {

  private val watchFibers = ConcurrentMap.empty[(Namespace, Name), (Watch, Fiber.Runtime[_, _])].runUIO

  /**
   * Monitor the status of Flink SessionJob K8s CR for a specified namespace and name.
   */
  // noinspection DuplicatedCode
  def watch(namespace: String, name: String) = {
    watchFibers.get((namespace, name)).flatMap {
      case Some(_) => ZIO.unit
      case None    =>
        launchProc(namespace, name)
          .flatMap(hook => watchFibers.put((namespace, name), hook))
          .unit
    }
  }

  // noinspection DuplicatedCode
  def unWatch(namespace: String, name: String): UIO[Unit] = {
    watchFibers.get((namespace, name)).flatMap {
      case None                 => ZIO.unit
      case Some((watch, fiber)) =>
        watchFibers.remove((namespace, name)) *>
        ZIO.attempt(watch.close()).ignore *>
        fiber.interrupt.unit
    }
  }

  private def launchProc(namespace: String, name: String) =
    for {
      watcher <- watchK8sResource { client =>
                   client
                     .resources(classOf[FlinkSessionJob])
                     .inNamespace(namespace)
                     .withName(name)
                 }
      fiber   <- watcher.stream
                   // Eval SessionJobCR status
                   .map((action, cr) => SessionJobCRStatus.eval(action, cr) -> Option(cr.getStatus.getJobStatus).map(JobStatus.fromCR))
                   // Update SessionJobCR status cache
                   .tap(status => sessionJobCRSnaps.put((namespace, name), status))
                   .runDrain
                   .forkDaemon
    } yield (watcher.watch, fiber)

}
