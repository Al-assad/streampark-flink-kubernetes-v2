package org.apache.streampark.flink.kubernetes

import zio.{durationInt, Duration}

import scala.annotation.unused

package object observer {

  // TODO make these configurable.
  val evalJobSnapParallelism: Int   = 5
  val evalJobSnapInterval: Duration = 1.seconds

  val restPollingInterval: Duration = 1.seconds
  val restRetryInterval: Duration   = 2.seconds

  val reachFlinkRestType: AccessFlinkRestType = AccessFlinkRestType.IP

  enum AccessFlinkRestType:
    case DNS, IP
}
