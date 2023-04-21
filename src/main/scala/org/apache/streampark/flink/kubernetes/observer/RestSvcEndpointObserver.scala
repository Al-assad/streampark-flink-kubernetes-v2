package org.apache.streampark.flink.kubernetes.observer

import io.fabric8.kubernetes.client.{Watch, Watcher}
import org.apache.streampark.flink.kubernetes.K8sTools.watchK8sResource
import org.apache.streampark.flink.kubernetes.model.RestSvcEndpoint
import org.apache.streampark.flink.kubernetes.observer.{Name, Namespace}
import zio.concurrent.ConcurrentMap
import zio.{Fiber, IO, Task, UIO, ZIO}
import org.apache.streampark.flink.kubernetes.runNow

import scala.jdk.CollectionConverters.*

class RestSvcEndpointObserver(
    restSvcEndpointSnaps: ConcurrentMap[(Namespace, Name), RestSvcEndpoint]) {

  private val watchFibers = ConcurrentMap.empty[(Namespace, Name), (Watch, Fiber.Runtime[_, _])].runNow

  /**
   * Monitor Flink REST service resources under the specified namespace and name.
   */
  def watch(namespace: String, name: String): IO[Throwable, Unit] = watchFibers.get((namespace, name)).flatMap {
    case Some(_) => ZIO.unit
    case None    =>
      launchProc(namespace, name)
        .flatMap(hook => watchFibers.put((namespace, name), hook))
        .unit
  }

  /**
   * Cancel monitoring of the specified Flink rest svc.
   */
  def unWatch(namespace: String, name: String): UIO[Unit] = watchFibers.get((namespace, name)).flatMap {
    case None                 => ZIO.unit
    case Some((watch, fiber)) =>
      watchFibers.remove((namespace, name)) *>
      ZIO.attempt(watch.close()).ignore *>
      fiber.interrupt *>
      restSvcEndpointSnaps.remove((namespace, name)).unit
  }

  private def launchProc(namespace: String, name: String): Task[(Watch, Fiber.Runtime[Nothing, Unit])] = {
    for {
      watcher <- watchK8sResource { client =>
                   client
                     .services()
                     .inNamespace(namespace)
                     .withName(s"$name-rest")
                 }
      fiber   <- watcher.stream
                   .map {
                     case (Watcher.Action.DELETED, _) => None
                     case (_, svc)                    =>
                       val namespace = svc.getMetadata.getNamespace
                       val name      = svc.getMetadata.getName
                       val clusterIP = svc.getSpec.getClusterIP
                       val port      = svc.getSpec.getPorts.asScala
                         .find(_.getPort == 8081)
                         .map(_.getTargetPort.getIntVal.toInt)
                         .getOrElse(8081)
                       Some(RestSvcEndpoint(namespace, name, port, clusterIP))
                   }
                   .mapZIO {
                     case None           => restSvcEndpointSnaps.remove((namespace, name))
                     case Some(endpoint) => restSvcEndpointSnaps.put((namespace, name), endpoint)
                   }
                   .runDrain
                   .forkDaemon
    } yield (watcher.watch, fiber)
  }

}
