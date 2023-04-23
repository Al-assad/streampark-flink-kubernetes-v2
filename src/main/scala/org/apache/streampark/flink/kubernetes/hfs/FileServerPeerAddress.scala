package org.apache.streampark.flink.kubernetes.hfs

import org.apache.streampark.flink.kubernetes.tool.K8sTools.{newK8sClient, usingK8sClient}
import org.apache.streampark.flink.kubernetes.tool.{runIO, runUIO}
import zio.logging.backend.SLF4J
import zio.{durationInt, Ref, UIO, ZIO}

import java.net.{InetAddress, InetSocketAddress, Socket}
import scala.util.Using

object FileServerPeerAddress {

  private val address: Ref[Option[String]] = Ref.make(None).runUIO

  // Auto calculate address when initialized.
  infer
    .map(Some(_))
    .tap(address.set)
    .tap(addr => ZIO.logInfo(s"[StreamPark] HTTP file server K8s peer address: $addr"))
    .forkDaemon
    .runUIO

  /**
   * Get the peer communication address snapshot.
   */
  def get: UIO[Option[String]] = address.get

  /**
   * Get the address, blocking the caller until the address is calculated.
   */
  def getEnsure: UIO[String] = address.get.flatMap {
    case None       => getEnsure.delay(100.millis)
    case Some(addr) => ZIO.succeed(addr)
  }

  /**
   * Refresh the peer communication address.
   */
  def refresh: UIO[Unit] = infer.tap(r => address.set(Some(r))).unit

  /**
   * Infer the relative file service peer address for k8s resources.
   */
  def infer: UIO[String] = {
    inferInsidePod.some
      .orElse(inferSocketReplyFromK8sApiServer.some)
      .orElse(directLocalHost.some)
      .orElse(ZIO.succeed("127.0.0.1"))
  }

  private def inferInsidePod: UIO[Option[String]] =
    usingK8sClient { client =>
      Option(client.getNamespace).flatMap { ns =>
        Option(
          client.services
            .inNamespace(ns)
            .withName("streampark-service")
            .get())
          .map(_ => s"streampark-service.$ns")
      }
    }.catchAll(_ => ZIO.succeed(None))

  private def inferSocketReplyFromK8sApiServer: UIO[Option[String]] =
    ZIO
      .attemptBlocking {
        val masterUrl = newK8sClient.getConfiguration.getMasterUrl

        extractHostPortFromUrl(masterUrl).flatMap { (host, port) =>
          Using(new Socket()) { socket =>
            socket.connect(new InetSocketAddress(host, port))
            socket.getLocalSocketAddress.asInstanceOf[InetSocketAddress].getAddress.getHostAddress
          }.toOption
        }
      }
      .catchAll(_ => ZIO.succeed(None))

  private def directLocalHost: UIO[Option[String]] =
    ZIO
      .attemptBlocking(InetAddress.getLocalHost.getHostAddress)
      .map(Some(_))
      .catchAll(_ => ZIO.succeed(None))

  private def extractHostPortFromUrl(url: String): Option[(String, Int)] = {
    val p1 = url.split("://")
    if p1.length != 2 then None
    else {
      val protocol = p1(0)
      val p2       = p1(1).split("/").head.split(":")
      if p2.length == 2 then Some(p2(0) -> p2(1).toInt)
      else if p2.length == 1 then
        protocol match {
          case "http"  => Some(p2(0) -> 80)
          case "https" => Some(p2(0) -> 443)
          case _       => None
        }
      else None
    }
  }

}
