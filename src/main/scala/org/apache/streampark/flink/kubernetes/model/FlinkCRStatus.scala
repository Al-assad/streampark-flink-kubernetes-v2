package org.apache.streampark.flink.kubernetes.model

import io.fabric8.kubernetes.client.Watcher
import org.apache.flink.kubernetes.operator.api.lifecycle.ResourceLifecycleState
import org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus
import org.apache.flink.kubernetes.operator.api.{FlinkDeployment, FlinkSessionJob}

/**
 * Flink K8s custom resource status.
 */
sealed trait FlinkCRStatus {
  val namespace: String
  val name: String
  val evalState: EvalState
  val error: Option[String]
  val updatedTs: Long
}

/**
 * Evaluated status for Flink K8s CR.
 */
enum EvalState:
  case DEPLOYING, READY, SUSPENDED, FAILED, DELETED

/**
 * Flink deployment CR status.
 * See: [[org.apache.flink.kubernetes.operator.api.FlinkDeployment]]
 */
case class DeployCRStatus(
    namespace: String,
    name: String,
    evalState: EvalState,
    action: Watcher.Action,
    lifecycle: ResourceLifecycleState,
    jmDeployStatus: JobManagerDeploymentStatus,
    error: Option[String] = None,
    updatedTs: Long)
    extends FlinkCRStatus

object DeployCRStatus {

  import JobManagerDeploymentStatus.*
  import ResourceLifecycleState.*

  def eval(action: Watcher.Action, cr: FlinkDeployment): DeployCRStatus = {
    val metadata       = cr.getMetadata
    val status         = cr.getStatus
    val lifecycle      = status.getLifecycleState
    val jmDeployStatus = status.getJobManagerDeploymentStatus

    val evalState = (action, lifecycle, jmDeployStatus) match
      case (Watcher.Action.DELETED, _, _) => EvalState.DELETED
      case (_, FAILED, _) | (_, _, ERROR) => EvalState.FAILED
      case (_, SUSPENDED, _)              => EvalState.SUSPENDED
      case (_, _, READY)                  => EvalState.READY
      case _                              => EvalState.DEPLOYING

    DeployCRStatus(
      namespace = metadata.getNamespace,
      name = metadata.getName,
      evalState = evalState,
      action = action,
      lifecycle = lifecycle,
      jmDeployStatus = jmDeployStatus,
      error = Option(cr.getStatus.getError),
      updatedTs = System.currentTimeMillis)
  }
}

/**
 * Flink Session Job CR status.
 * See: [[org.apache.flink.kubernetes.operator.api.FlinkSessionJob]]
 */
case class SessionJobCRStatus(
    namespace: String,
    name: String,
    refDeployName: String,
    evalState: EvalState,
    action: Watcher.Action,
    lifecycle: ResourceLifecycleState,
    error: Option[String],
    updatedTs: Long)
    extends FlinkCRStatus

object SessionJobCRStatus {
  import ResourceLifecycleState.*

  def eval(action: Watcher.Action, cr: FlinkSessionJob): SessionJobCRStatus = {
    val metadata  = cr.getMetadata
    val status    = cr.getStatus
    val lifecycle = status.getLifecycleState

    val evalState = (action, lifecycle) match
      case (Watcher.Action.DELETED, _)    => EvalState.DELETED
      case (_, STABLE) | (_, ROLLED_BACK) => EvalState.READY
      case (_, SUSPENDED)                 => EvalState.SUSPENDED
      case (_, FAILED)                    => EvalState.FAILED
      case _                              => EvalState.DEPLOYING

    SessionJobCRStatus(
      namespace = metadata.getNamespace,
      name = metadata.getName,
      refDeployName = cr.getSpec.getDeploymentName,
      evalState = evalState,
      action = action,
      lifecycle = lifecycle,
      error = Option(cr.getStatus.getError),
      updatedTs = System.currentTimeMillis)
  }
}
