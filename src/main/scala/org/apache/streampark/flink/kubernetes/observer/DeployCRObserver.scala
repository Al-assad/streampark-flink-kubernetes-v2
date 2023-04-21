package org.apache.streampark.flink.kubernetes.observer

import io.fabric8.kubernetes.client.Watch
import org.apache.flink.kubernetes.operator.api.FlinkDeployment
import org.apache.streampark.flink.kubernetes.K8sTools.{usingK8sClient, watchK8sResource}
import org.apache.streampark.flink.kubernetes.model.{DeployCRStatus, JobSnapshot, JobStatus}
import org.apache.streampark.flink.kubernetes.runNow
import zio.{Fiber, IO, UIO, ZIO}
import zio.concurrent.ConcurrentMap

class DeployCRObserver(
    deployCRSnaps: ConcurrentMap[(Namespace, Name), (DeployCRStatus, Option[JobStatus])]) {

  private val watchFibers = ConcurrentMap.empty[(Namespace, Name), (Watch, Fiber.Runtime[_, _])].runNow

  /**
   * Monitor the status of K8s FlinkDeployment CR for a specified namespace and name.
   */
  def watch(namespace: String, name: String) = {
    watchFibers.get((namespace, name)).flatMap {
      case Some(_) => ZIO.unit
      case None    =>
        launchProc(namespace, name)
          .flatMap(hook => watchFibers.put((namespace, name), hook))
          .unit
          .whenZIO(existCr(namespace, name))
    }
  }

  def unWatch(namespace: String, name: String): UIO[Unit] = {
    watchFibers.get((namespace, name)).flatMap {
      case None                 => ZIO.unit
      case Some((watch, fiber)) =>
        watchFibers.remove((namespace, name)) *>
        ZIO.attempt(watch.close()).ignore *>
        fiber.interrupt.unit
    }
  }

  private def existCr(namespace: String, name: String): IO[Throwable, Boolean] =
    usingK8sClient { client =>
      client
        .resources(classOf[FlinkDeployment])
        .inNamespace(namespace)
        .withName(name)
        .get != null
    }

  private def launchProc(namespace: String, name: String) =
    for {
      watcher <- watchK8sResource { client =>
                   client
                     .resources(classOf[FlinkDeployment])
                     .inNamespace(namespace)
                     .withName(name)
                 }
      fiber   <-
        watcher.stream
          // Eval FlinkDeployment status
          .map((action, deployment) => DeployCRStatus.eval(action, deployment) -> Option(deployment.getStatus.getJobStatus).map(JobStatus.fromCR))
          // Update FlinkDeployment status cache
          .tap(status => deployCRSnaps.put((namespace, name), status))
          .runDrain
          .forkDaemon
    } yield (watcher.watch, fiber)

}
