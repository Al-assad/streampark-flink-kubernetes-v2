package org.apache.streampark.flink.kubernetes

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator.Feature
import io.fabric8.kubernetes.api.model.ObjectMeta
import org.apache.flink.kubernetes.operator.api.{FlinkDeployment, FlinkDeploymentList}
import org.apache.flink.kubernetes.operator.api.spec.{FlinkDeploymentSpec, FlinkVersion, JobManagerSpec, JobSpec, JobState, Resource, TaskManagerSpec, UpgradeMode}
import org.apache.streampark.flink.kubernetes.tool.K8sTools.newK8sClient
import org.apache.streampark.flink.kubernetes.model.{FlinkSessionJobDef, JobDef}
import org.apache.streampark.flink.kubernetes.tool.yamlMapper

import util.chaining.scalaUtilChainingOps
import scala.jdk.CollectionConverters.*

@main def test234 = {

//  val client = newK8sClient

  val spec = FlinkDeploymentSpec().tap { s =>
    s.setImage("flink:1.16")
    s.setFlinkVersion(FlinkVersion.v1_16)
    s.setFlinkConfiguration(Map("taskmanager.numberOfTaskSlots" -> "2").asJava)
    s.setServiceAccount("flink")

    s.setJobManager(JobManagerSpec().tap { s =>
      s.setResource(Resource().tap { s =>
        s.setCpu(1.0)
        s.setMemory("2048m")
      })
    })
    s.setTaskManager(TaskManagerSpec().tap { s =>
      s.setResource(Resource().tap { s =>
        s.setCpu(1.0)
        s.setMemory("2048m")
      })
    })

    s.setJob(JobSpec().tap { s =>
      s.setJarURI("local:///opt/flink/examples/streaming/StateMachineExample.jar")
      s.setParallelism(2)
      s.setUpgradeMode(UpgradeMode.STATELESS)
      s.setState(JobState.RUNNING)
    })
  }

  val deploy = FlinkDeployment()
  deploy.setSpec(spec)
  deploy.setMetadata {
    val meta = ObjectMeta()
    meta.setName("basic-example-2")
    meta.setNamespace("fdev")
    meta
  }

  println(deploy)

  val client = newK8sClient

  client
    .resource(deploy)
    .create();

//  spec.setImage("flink:1.16")
//  spec.setFlinkVersion(FlinkVersion.v1_16)
//  spec.setFlinkConfiguration(Map("taskmanager.numberOfTaskSlots" -> "2").asJava)
//  spec.setServiceAccount("flink")
//  spec.setJobManager(
//    JobManagerSpec().contra {
//      _.setRep
//    }
//  )

//  client
//    .resources(classOf[FlinkDeployment])
//    .load()
}

//@main def test33 = zioRun {
//  for {
//    _ <- FlinkK8sObserver.track(TrackKey.appJob(114514, "fdev", "basic-example"))
//    _ <- FlinkK8sObserver.evaluatedJobSnaps.subValues().map(_.prettyStr).debug("job-snapshot").runDrain.fork
////    _ <- FlinkK8sObserver.deployCRSnaps.subValues().map(_.prettyStr).debug("deploy-cr").runDrain.fork
////    _ <- FlinkK8sObserver.sessionJobCRSnaps.subValues().map(_.prettyStr).debug("sessionjob-cr").runDrain.fork
////    _ <- FlinkK8sObserver.clusterJobStatusSnaps.subValues().map(_.prettyStr).debug("job-status").runDrain.fork
//    _ <- ZIO.never
//  } yield ()
//}

@main def test66(): Unit = {

  val sessionJob = FlinkSessionJobDef(
    namespace = "fdev",
    name = "job-1",
    deploymentName = "cluster-1",
    job = JobDef(
      jarURI = "local:///opt/flink/examples/streaming/StateMachineExample.jar",
      parallelism = 2
    )
  ).toFlinkSessionJob

  val yaml = yamlMapper.writeValueAsString(sessionJob)

  println(yaml)

}
