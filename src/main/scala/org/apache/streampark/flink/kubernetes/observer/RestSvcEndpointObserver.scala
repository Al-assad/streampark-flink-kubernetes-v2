package org.apache.streampark.flink.kubernetes.observer

import io.fabric8.kubernetes.api.model.Service
import io.fabric8.kubernetes.client.{Watch, Watcher}
import org.apache.streampark.flink.kubernetes.K8sTools
import org.apache.streampark.flink.kubernetes.K8sTools.{watchK8sResource, watchK8sResourceForever, K8sResourceWatcher}
import org.apache.streampark.flink.kubernetes.model.RestSvcEndpoint
import org.apache.streampark.flink.kubernetes.observer.{Name, Namespace}
import zio.concurrent.ConcurrentMap
import zio.{Fiber, IO, Task, UIO, ZIO}
import org.apache.streampark.flink.kubernetes.util.runUIO
import zio.ZIO.logInfo

import scala.jdk.CollectionConverters.*

class RestSvcEndpointObserver(
    restSvcEndpointSnaps: ConcurrentMap[(Namespace, Name), RestSvcEndpoint]) {

  private val watchers = ConcurrentMap.empty[(Namespace, Name), K8sResourceWatcher[Service]].runUIO

  /**
   * Monitor Flink REST service resources under the specified namespace and name.
   */
  def watch(namespace: String, name: String): UIO[Unit] =
    watchers.get((namespace, name)).flatMap {
      case Some(_) => ZIO.unit
      case None    =>
        val watch = launchProc(namespace, name)
        watchers.put((namespace, name), watch) *>
        watch.launch
    }

  /**
   * Cancel monitoring of the specified Flink rest svc.
   */
  def unWatch(namespace: String, name: String): UIO[Unit] =
    watchers.get((namespace, name)).flatMap {
      case None          => ZIO.unit
      case Some(watcher) =>
        watcher.stop *>
        watchers.remove((namespace, name)) *>
        restSvcEndpointSnaps.remove((namespace, name)).unit
    }

  private def launchProc(namespace: String, name: String): K8sResourceWatcher[Service] =
    watchK8sResourceForever { client =>
      client
        .services()
        .inNamespace(namespace)
        .withName(s"$name-rest")
    } { stream =>
      stream
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
    }

}
