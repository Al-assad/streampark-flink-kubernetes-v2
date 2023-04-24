package org.apache.streampark.flink.kubernetes.hfs

import zio.{UIO, ZIO}
import zio.http.*

object FileServer {

  private val routes = Http.collectHttp[Request] {
    case Method.GET -> !! / "health"         => Handler.ok.toHttp
    case Method.GET -> !! / "fs" / subspace / name => Http.fromFileZIO(FileMirror.getLocalFile(subspace, name))
  }

  /**
   * Launch the netty-based internal http file server at port specified by fileServerPort param.
   */
  def launch: UIO[Unit] =
    for {
      _ <- ZIO.log(s"[StreamPark] Launch internal http file server at port: ${fileServerPort}")
      _ <- Server
             .serve(routes.withDefaultErrorResponse)
             .provide(Server.defaultWithPort(fileServerPort))
             .forkDaemon
    } yield ()

}
