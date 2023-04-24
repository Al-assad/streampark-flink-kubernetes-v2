package org.apache.streampark.flink.kubernetes.model

import zio.json.JsonCodec

case class JobSavepointDef(
    drain: Boolean = false,
    savepointPath: Option[String] = None,
    formatType: Option[String] = None,
    triggerId: Option[String] = None)

object JobSavepointDef {
  val CANONICAL_FORMAT = "CANONICAL"
  val NATIVE_FORMAT    = "NATIVE"
}
