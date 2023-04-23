package org.apache.streampark.flink.kubernetes.hfs

import zio.{IO, Task, UIO, ZIO}

import java.io.File

object FileMirror {

  private val mirrorRoot = os.Path(File(localMirrorDir).getAbsolutePath)

  /**
   * Mirror the file to local mirror directory.
   * Return tuple (namespace, file-name).
   */
  def mirror(srcFilePath: String, ns: String): IO[Throwable, (String, String)] = ZIO.attemptBlocking {
    val srcPath  = os.Path(srcFilePath)
    val fileName = srcPath.last
    os.copy(
      from = srcPath,
      to = mirrorRoot / ns / fileName,
      replaceExisting = true,
      createFolders = true,
      mergeFolders = true
    )
    ns -> fileName
  }

  /**
   * Get the http access url of the mirrored file resource.
   */
  def getHttpAccessUrl(ns: String, name: String): UIO[String] = {
    for {
      httpHost <- FileServerPeerAddress.getEnsure
      url       = s"http://$httpHost:$fileServerPort/$ns/$name"
    } yield url
  }

  /**
   * Get the local File of the mirrored file resource.
   */
  def getLocalFile(ns: String, name: String): IO[Throwable, File] = {
    for {
      localFile: File <- ZIO.succeed((mirrorRoot / ns / name).toIO)
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
