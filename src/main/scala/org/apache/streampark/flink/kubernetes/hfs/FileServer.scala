package org.apache.streampark.flink.kubernetes.hfs

import org.apache.streampark.flink.kubernetes.hfs.FileMirror.{FileNotFound, NotAFile}
import zio.{Ref, UIO, ZIO}
import zio.http.*
import org.apache.streampark.flink.kubernetes.util.runUIO

object FileServer {

  private val routes = Http.collectHttp[Request] {
    case Method.GET -> !! / "health"               => Handler.ok.toHttp
    case Method.GET -> !! / "fs" / subspace / name => Http.fromFileZIO(FileMirror.getLocalFile(subspace, name))
  }

  private val isLaunch: Ref[Boolean] = Ref.make(false).runUIO

  /**
   * Launch the netty-based internal http file server at port specified by fileServerPort param.
   */
  def launch: UIO[Unit] = {
    val serve = for {
      _ <- ZIO.log(s"[StreamPark] Launch internal http file server at port: ${fileServerPort}")
      _ <- Server
             .serve(routes.withDefaultErrorResponse)
             .provide(Server.defaultWithPort(fileServerPort))
             .forkDaemon
    } yield ()
    (serve *> isLaunch.set(true)).unlessZIO(isLaunch.get).unit
  }

}
