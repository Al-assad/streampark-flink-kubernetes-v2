package org.apache.streampark.flink.kubernetes

import io.fabric8.kubernetes.api.model.ReplicationControllerBuilder
import io.fabric8.kubernetes.client.KubernetesClient
import org.apache.flink.kubernetes.operator.api.FlinkDeployment
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion
import org.apache.streampark.flink.kubernetes.K8sTools.newK8sClient
import org.apache.streampark.flink.kubernetes.model.*
import org.apache.streampark.flink.kubernetes.util.liftValueAsSome
import scala.jdk.CollectionConverters.*

//@main def testObserver = zioRun {
//  for {
//    _ <- FlinkK8sObserver.track(TrackKey.appJob(114514, "fdev", "basic-example"))
//    _ <- FlinkK8sObserver.evaluatedJobSnaps.subValues().map(_.prettyStr).debug("job-snapshot").runDrain.fork
////    _ <- FlinkK8sObserver.deployCRSnaps.subValues().map(_.prettyStr).debug("deploy-cr").runDrain.fork
////    _ <- FlinkK8sObserver.sessionJobCRSnaps.subValues().map(_.prettyStr).debug("sessionjob-cr").runDrain.fork
////    _ <- FlinkK8sObserver.clusterJobStatusSnaps.subValues().map(_.prettyStr).debug("job-status").runDrain.fork
//    _ <- ZIO.never
//  } yield ()
//}

@main def test = {
  val client = newK8sClient

  val a = FlinkDeploymentDef(
    name = "basic-2",
    namespace = "fdev",
    image = "flink:1.16",
    flinkVersion = FlinkVersion.v1_16,
    jobManager = JobManagerDef(cpu = 1, memory = "1024m"),
    taskManager = TaskManagerDef(cpu = 1, memory = "1024m"),
    job = JobDef(
      jarURI = "local:///opt/flink/examples/streaming/StateMachineExample.jar",
      parallelism = 1
    ),
    ingress = IngressDef.simplePathBased
  ).toFlinkDeployment

  client.resource(a).create
//  client.resource(a).update()

  client.close()
}

@main def test2 = {
  val client = newK8sClient
  client
    .resources(classOf[FlinkDeployment])
    .inNamespace("basic-2")
    .withName("basic-example")
    .delete()

  client.close()

}
