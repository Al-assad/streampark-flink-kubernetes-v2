package org.apache.streampark.flink.kubernetes.model

import io.fabric8.kubernetes.api.model.{ObjectMeta, Pod}
import org.apache.flink.kubernetes.operator.api.FlinkDeployment
import org.apache.flink.kubernetes.operator.api.spec.*

import scala.jdk.CollectionConverters.*

/**
 * Typed-safe Mirror of [[org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec]]
 */
case class FlinkDeploymentDef(
    namespace: String,
    name: String,
    image: String,
    imagePullPolicy: Option[String] = None,
    serviceAccount: String = "flink",
    flinkVersion: FlinkVersion,
    jobManager: JobManagerDef,
    taskManager: TaskManagerDef,
    restartNonce: Option[Long] = None,
    flinkConfiguration: Map[String, String] = Map.empty,
    logConfiguration: Map[String, String] = Map.empty,
    podTemplate: Option[Pod] = None,
    ingress: Option[IngressDef] = None,
    mode: KubernetesDeploymentMode = KubernetesDeploymentMode.NATIVE,
    job: Option[JobDef] = None,
    extJarPaths: Array[String] = Array.empty) {

  // noinspection DuplicatedCode
  def toFlinkDeployment: FlinkDeployment = {
    val spec = FlinkDeploymentSpec()

    spec.setImage(image)
    imagePullPolicy.foreach(spec.setImagePullPolicy)
    spec.setServiceAccount(serviceAccount)
    spec.setFlinkVersion(flinkVersion)
    restartNonce.foreach(spec.setRestartNonce(_))
    podTemplate.foreach(spec.setPodTemplate)
    spec.setMode(mode)

    val jmSpec = jobManager.toJobManagerSpec
    spec.setJobManager(jmSpec)
    val tmSpec = taskManager.toTaskManagerSpec
    spec.setTaskManager(tmSpec)

    if flinkConfiguration.nonEmpty then spec.setFlinkConfiguration(flinkConfiguration.asJava)
    if logConfiguration.nonEmpty then spec.setLogConfiguration(logConfiguration.asJava)

    ingress.map(_.toIngressSpec).foreach(spec.setIngress)
    job.map(_.toJobSpec).foreach(spec.setJob)

    val deployment = FlinkDeployment()
    val metadata   = ObjectMeta()
    metadata.setNamespace(namespace)
    metadata.setName(name)
    deployment.setMetadata(metadata)
    deployment.setSpec(spec)
    deployment.setStatus(null)
    deployment
  }
}

case class JobManagerDef(
    cpu: Double,
    memory: String,
    replicas: Int = 1,
    podTemplate: Option[Pod] = None) {

  def toJobManagerSpec: JobManagerSpec = {
    val spec = JobManagerSpec()
    spec.setResource(Resource(cpu, memory))
    spec.setReplicas(replicas)
    podTemplate.foreach(spec.setPodTemplate)
    spec
  }
}

case class TaskManagerDef(
    cpu: Double,
    memory: String,
    replicas: Option[Int] = None,
    podTemplate: Option[Pod] = None) {

  def toTaskManagerSpec: TaskManagerSpec = {
    val spec = TaskManagerSpec()
    spec.setResource(Resource(cpu, memory))
    replicas.foreach(spec.setReplicas(_))
    podTemplate.foreach(spec.setPodTemplate)
    spec
  }
}

case class IngressDef(
    template: String,
    className: String,
    annotations: Map[String, String] = Map.empty) {

  def toIngressSpec: IngressSpec = {
    val spec = IngressSpec()
    spec.setTemplate(template)
    spec.setClassName(className)
    if annotations.nonEmpty then spec.setAnnotations(annotations.asJava)
    spec
  }
}

case class JobDef(
    jarURI: String,
    parallelism: Int,
    entryClass: Option[String] = None,
    args: Array[String] = Array.empty,
    state: JobState = JobState.RUNNING,
    upgradeMode: UpgradeMode = UpgradeMode.STATELESS,
    savepointTriggerNonce: Option[Long] = None,
    initialSavepointPath: Option[String] = None,
    allowNonRestoredState: Option[Boolean] = None) {

  def toJobSpec: JobSpec = {
    val spec = JobSpec()
    spec.setJarURI(jarURI)
    spec.setParallelism(parallelism)
    entryClass.foreach(spec.setEntryClass)
    if args.nonEmpty then spec.setArgs(args)
    spec.setState(state)
    spec.setUpgradeMode(upgradeMode)

    savepointTriggerNonce.foreach(spec.setSavepointTriggerNonce(_))
    initialSavepointPath.foreach(spec.setInitialSavepointPath)
    allowNonRestoredState.foreach(spec.setAllowNonRestoredState(_))
    spec
  }
}
