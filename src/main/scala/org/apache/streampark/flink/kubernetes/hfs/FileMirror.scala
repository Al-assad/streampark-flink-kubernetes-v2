package org.apache.streampark.flink.kubernetes.hfs

import zio.{IO, Task, UIO, ZIO}

import java.io.File

object FileMirror {

  private val mirrorRoot = os.Path(File(localMirrorDir).getAbsolutePath)

  /**
   * Mirror the file to local mirror directory.
   * Return tuple (namespace, file-name).
   */
  def mirror(srcFilePath: String, subspace: String): IO[Throwable, (String, String)] = ZIO.attemptBlocking {
    val srcPath  = os.Path(File(srcFilePath).getAbsolutePath)
    val fileName = srcPath.last
    os.copy(
      from = srcPath,
      to = mirrorRoot / subspace / fileName,
      replaceExisting = true,
      createFolders = true,
      mergeFolders = true
    )
    subspace -> fileName
  }

  /**
   * Get the http access url of the mirrored file resource.
   */
  def getHttpUrl(subspace: String, name: String): UIO[String] = {
    for {
      httpHost <- FileServerPeerAddress.getEnsure
      url       = s"http://$httpHost:$fileServerPort/fs/$subspace/$name"
    } yield url
  }

  def mirrorAndGetHttpUrl(srcFilePath: String, ns: String): ZIO[Any, Throwable, String] =
    mirror(srcFilePath, ns).flatMap((ns, name) => getHttpUrl(ns, name))

  /**
   * Get the local File of the mirrored file resource.
   */
  def getLocalFile(subspace: String, name: String): IO[Throwable, File] = {
    for {
      localFile: File <- ZIO.succeed((mirrorRoot / subspace / name).toIO)
      _               <- ZIO
                           .fail(FileNotFound(localFile.getAbsolutePath))
                           .whenZIO(ZIO.attempt(localFile.exists()).map(!_))
      _               <- ZIO
                           .fail(NotAFile(localFile.getAbsolutePath))
                           .whenZIO(ZIO.attempt(localFile.isFile).map(!_))
    } yield localFile
  }

  case class FileNotFound(path: String) extends Exception(s"File not found: $path")
  case class NotAFile(path: String)     extends Exception(s"Not a file: $path")

}
