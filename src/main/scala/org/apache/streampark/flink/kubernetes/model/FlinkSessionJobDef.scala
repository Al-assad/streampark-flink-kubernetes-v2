package org.apache.streampark.flink.kubernetes.model

import io.fabric8.kubernetes.api.model.ObjectMeta
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob
import org.apache.flink.kubernetes.operator.api.spec.{FlinkSessionJobSpec, JobSpec}

import scala.jdk.CollectionConverters.*

/**
 *  Typed-safe Mirror of [[org.apache.flink.kubernetes.operator.api.spec.FlinkSessionJobSpec]]
 */
case class FlinkSessionJobDef(
    namespace: String,
    name: String,
    deploymentName: String,
    job: JobDef,
    flinkConfiguration: Map[String, String] = Map.empty,
    restartNonce: Option[Long] = None) {

  def toFlinkSessionJob: FlinkSessionJob = {
    val spec = FlinkSessionJobSpec()
    spec.setDeploymentName(deploymentName)
    spec.setJob(job.toJobSpec)
    if flinkConfiguration.nonEmpty then spec.setFlinkConfiguration(flinkConfiguration.asJava)
    restartNonce.foreach(spec.setRestartNonce(_))

    val sessionJob = FlinkSessionJob()
    val metadata   = ObjectMeta()
    metadata.setNamespace(namespace)
    metadata.setName(name)
    sessionJob.setMetadata(metadata)
    sessionJob.setSpec(spec)
    sessionJob.setStatus(null)
    sessionJob
  }
}
