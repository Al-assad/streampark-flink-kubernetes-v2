package org.apache.streampark.flink.kubernetes

import com.typesafe.scalalogging.Logger
import io.fabric8.kubernetes.client.*
import io.fabric8.kubernetes.client.dsl.WatchAndWaitable
import org.apache.streampark.flink.kubernetes.util.{runIO, runUIO, unsafeRun}
import zio.stream.{UStream, ZStream}
import zio.{durationInt, Duration, Fiber, IO, Promise, Queue, Ref, Schedule, UIO, ZIO}

object K8sTools {

  /**
   * Create new fabric8 k8s client
   */
  def newK8sClient: KubernetesClient = new KubernetesClientBuilder().build

  inline def usingK8sClient[A](f: KubernetesClient => A): IO[Throwable, A] = ZIO.scoped {
    ZIO
      .acquireRelease(ZIO.attempt(newK8sClient))(client => ZIO.attempt(client.close()).ignore)
      .flatMap(client => ZIO.attemptBlocking(f(client)))
  }

  /**
   * Converts fabric8 callback-style Watch to ZStream style.
   */
  inline def watchK8sResource[R](genWatch: KubernetesClient => WatchAndWaitable[R]) = {
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
      stream   = ZStream.fromQueue(queue)
    } yield K8sWatcher(watch, stream)
  }

  case class K8sWatcher[R](watch: Watch, stream: UStream[(Watcher.Action, R)])

  /**
   * Safely and automatically retry subscriptions to k8s resources。
   */
  def watchK8sResourceForever[R](
      genWatch: KubernetesClient => WatchAndWaitable[R]
    )(pipe: UStream[(Watcher.Action, R)] => UStream[_]): K8sResourceWatcher[R] = {
    K8sResourceWatcher(genWatch, pipe)
  }

  case class K8sResourceWatcher[R](genWatch: KubernetesClient => WatchAndWaitable[R], pipe: UStream[(Watcher.Action, R)] => UStream[_]) {
    private val queue: Queue[(Watcher.Action, R)]                 = Queue.unbounded[(Watcher.Action, R)].runUIO
    private val clientRef: Ref[Option[KubernetesClient]]          = Ref.make(None).runUIO
    private val watchRef: Ref[Option[Watch]]                      = Ref.make(None).runUIO
    private val consumeFiberRef: Ref[Option[Fiber.Runtime[_, _]]] = Ref.make(None).runUIO
    private val mainFiberRef: Ref[Option[Fiber.Runtime[_, _]]]    = Ref.make(None).runUIO

    def launch: UIO[Unit] =
      for {
        fiber <- (innerStop *> innerRun).retry(Schedule.spaced(1.seconds)).forkDaemon
        _     <- mainFiberRef.set(Some(fiber))
      } yield ()

    // noinspection DuplicatedCode
    def stop: UIO[Unit] =
      for {
        _ <- innerStop
        _ <- mainFiberRef.get.flatMap {
               case Some(fiber) => fiber.interrupt
               case None        => ZIO.unit
             }
        _ <- queue.shutdown
      } yield ()

    private def innerRun: ZIO[Any, Throwable, Unit] =
      for {
        client <- ZIO.attempt(newK8sClient)
        _      <- clientRef.set(Some(client))

        watcherShape = new Watcher[R]() {
                         private val logger = Logger(getClass)

                         override def reconnecting(): Boolean = true

                         override def eventReceived(action: Watcher.Action, resource: R): Unit = {
                           queue.offer((action, resource)).runIO
                         }

                         override def onClose(cause: WatcherException): Unit = {
                           logger.error("[StreamPark] K8s Watcher was accidentally closed.", cause)
                           launch.runIO
                         }
                       }
        watch       <- ZIO.attemptBlocking(genWatch(client).watch(watcherShape))
        _           <- watchRef.set(Some(watch))

        fiber <- pipe(ZStream.fromQueue(queue)).runDrain.forkDaemon
        _     <- consumeFiberRef.set(Some(fiber))
      } yield ()

    // noinspection DuplicatedCode
    private def innerStop: UIO[Unit] =
      for {
        _ <- consumeFiberRef.get.flatMap {
               case Some(fiber) => fiber.interrupt
               case None        => ZIO.unit
             }
        _ <- watchRef.get.flatMap {
               case Some(watch) => ZIO.attemptBlocking(watch.close()).ignore
               case None        => ZIO.unit
             }
        _ <- clientRef.get.flatMap {
               case Some(client) => ZIO.attemptBlocking(client.close()).ignore
               case None         => ZIO.unit
             }
      } yield ()

  }

}
