package org.apache.streampark.flink.kubernetes

import com.typesafe.scalalogging.Logger
import io.fabric8.kubernetes.client.*
import io.fabric8.kubernetes.client.dsl.WatchAndWaitable
import zio.stream.{UStream, ZStream}
import zio.{IO, Queue, ZIO}
import org.apache.streampark.flink.kubernetes.util.runIO

object K8sTools {

  /**
   * Create new fabric8 k8s client
   */
  def newK8sClient: KubernetesClient = new KubernetesClientBuilder().build

  def usingK8sClient[A](f: KubernetesClient => A): IO[Throwable, A] = ZIO.scoped {
    ZIO
      .acquireRelease(ZIO.attempt(newK8sClient))(client => ZIO.attempt(client.close()).ignore)
      .flatMap(client => ZIO.attemptBlocking(f(client)))
  }

  /**
   * Converts fabric8 callback-style Watch to ZStream style.
   */
  def watchK8sResource[R](genWatch: KubernetesClient => WatchAndWaitable[R]) = {
    for {
      queue  <- Queue.unbounded[(Watcher.Action, R)]
      client <- ZIO.attempt(newK8sClient)

      watcherF = new Watcher[R]() {
                   private val logger = Logger(getClass)

                   override def reconnecting(): Boolean = true

                   override def eventReceived(action: Watcher.Action, resource: R): Unit = {
                     queue.offer((action, resource)).runIO
                   }

                   override def onClose(cause: WatcherException): Unit = {
                     logger.error("[StreamPark] K8s Watcher was accidentally closed.", cause)
                   }

                   override def onClose(): Unit = {
                     super.onClose()
                     queue.shutdown.runIO
                     client.close()
                   }
                 }
      watch   <- ZIO.attemptBlocking(genWatch(client).watch(watcherF))
      stream   = ZStream.fromQueueWithShutdown(queue)
    } yield K8sWatcher(watch, stream)
  }

  case class K8sWatcher[R](watch: Watch, stream: UStream[(Watcher.Action, R)])

}
