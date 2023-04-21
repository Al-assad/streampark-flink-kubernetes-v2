package org.apache.streampark.flink.kubernetes

import com.fasterxml.jackson.databind.ObjectMapper
import io.fabric8.kubernetes.api.model.ObjectMeta
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext
import io.fabric8.kubernetes.client.http.HttpClient
import io.fabric8.kubernetes.client.{KubernetesClient, KubernetesClientBuilder}
import okhttp3.logging.HttpLoggingInterceptor
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec
import org.apache.flink.kubernetes.operator.api.status.{FlinkDeploymentStatus, JobManagerDeploymentStatus}
import org.apache.flink.kubernetes.operator.api.{CrdConstants, FlinkDeployment}
import org.apache.streampark.flink.kubernetes.model.TrackKey
import org.apache.streampark.flink.kubernetes.observer.FlinkK8sObserver
import org.apache.streampark.flink.kubernetes.prettyStr
import org.slf4j.LoggerFactory
import zio.concurrent.ConcurrentSet
import zio.{durationInt, Schedule, Scope, ZIO}
import zio.stream.ZStream

import java.util
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Using}

def attachK8s[A](f: KubernetesClient => A): A = {
  Using(KubernetesClientBuilder().build)(f) match
    case Success(value)     => value
    case Failure(exception) => throw exception
}

@main def test1 = {
  val client = KubernetesClientBuilder().build

  client
    .pods()
    .inNamespace("fdev")
    .list()
    .getItems
    .asScala
    .foreach(println)
}

@main def test21 = attachK8s { client =>

  val a = client
    .resources(classOf[FlinkDeployment])
    .inNamespace("fdev")
    .withName("basic-example")
    .get()

}

@main def test223 = attachK8s { client =>

  val a = client
    .resources(classOf[FlinkDeployment])
    .inNamespace("fdev")
    .list

  a.getItems.asScala.foreach(e => println(e))

}

@main def test33 = zioRun {
  for {
    _ <- FlinkK8sObserver.track(TrackKey.appJob(114514, "fdev", "basic-example"))
    _ <- FlinkK8sObserver.evaluatedJobSnaps.subValues().map(_.prettyStr).debug("job-snapshot").runDrain.fork
//    _ <- FlinkK8sObserver.deployCRSnaps.subValues().map(_.prettyStr).debug("deploy-cr").runDrain.fork
//    _ <- FlinkK8sObserver.sessionJobCRSnaps.subValues().map(_.prettyStr).debug("sessionjob-cr").runDrain.fork
//    _ <- FlinkK8sObserver.clusterJobStatusSnaps.subValues().map(_.prettyStr).debug("job-status").runDrain.fork
    _ <- ZIO.never
  } yield ()
}

