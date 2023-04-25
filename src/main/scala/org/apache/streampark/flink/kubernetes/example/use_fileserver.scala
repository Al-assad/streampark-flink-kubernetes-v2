package org.apache.streampark.flink.kubernetes.example

import org.apache.streampark.flink.kubernetes.hfs.{FileMirror, FileServer}
import org.apache.streampark.flink.kubernetes.util.unsafeRun
import zio.ZIO

/**
 * Launch FileServer and mirror the local files.
 *
 * Includes two routes：
 *  - /heath: head check
 *  - /fs/[subspace]/[file_name]: static file
 */
@main def standaloneFileServer = unsafeRun {
  for
    _ <- FileServer.launch

    // mirror local file to http filer server which can be any local path.
    _ <- FileMirror.mirror("assets/hi.md", "test")
    _ <- FileMirror.mirror("assets/flink-faker-0.5.3.jar", "test")

    // print the http url that corresponds to the accessible file.
    _ <- FileMirror.getHttpUrl("test", "hi.md").debug
    _ <- FileMirror.getHttpUrl("test", "flink-faker-0.5.3.jar").debug
    _ <- ZIO.never
  yield ()
}

/**
 * More simplified code of standaloneFileServer.
 */
@main def standaloneFileServer2 = unsafeRun {
  for
    _ <- FileServer.launch
    _ <- FileMirror.mirrorAndGetHttpUrl("assets/hi.md", "test").debug
    _ <- FileMirror.mirrorAndGetHttpUrl("assets/flink-faker-0.5.3.jar", "test").debug
    _ <- ZIO.never
  yield ()
}


