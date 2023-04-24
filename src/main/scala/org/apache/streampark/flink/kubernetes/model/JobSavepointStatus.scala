package org.apache.streampark.flink.kubernetes.model

case class JobSavepointStatus(state: FlinkPipeOprState, failureCause: Option[String], location: Option[String]) {
  lazy val isCompleted = state == FlinkPipeOprState.Completed
  lazy val isFailed    = failureCause.isDefined
}

enum FlinkPipeOprState(val rawValue: String):
  case Completed  extends FlinkPipeOprState("COMPLETED")
  case InProgress extends FlinkPipeOprState("IN_PROGRESS")
  case Unknown    extends FlinkPipeOprState("UNKNOWN")

object FlinkPipeOprStates:
  def ofRaw(rawValue: String): FlinkPipeOprState =
    FlinkPipeOprState.values.find(_.rawValue == rawValue).getOrElse(FlinkPipeOprState.Unknown)
