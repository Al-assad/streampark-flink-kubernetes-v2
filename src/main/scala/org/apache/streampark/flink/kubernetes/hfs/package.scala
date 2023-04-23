package org.apache.streampark.flink.kubernetes


package object hfs {

  // TODO make these configurable
  val localMirrorDir: String = "./hfs"
  val fileServerPort: Int    = 10030

}
