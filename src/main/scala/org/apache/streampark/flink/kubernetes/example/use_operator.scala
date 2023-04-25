package org.apache.streampark.flink.kubernetes.example
import org.apache.flink.kubernetes.operator.api.spec.{FlinkVersion, JobState}
import org.apache.streampark.flink.kubernetes.hfs.FileServer
import org.apache.streampark.flink.kubernetes.model.*
import org.apache.streampark.flink.kubernetes.observer.FlinkK8sObserver
import org.apache.streampark.flink.kubernetes.operator.FlinkK8sOperator
import org.apache.streampark.flink.kubernetes.util.{
  liftValueAsSome,
  runIO,
  unsafeRun,
  ConcurrentMapExtension,
  PrettyStringExtension,
  RefMapExtension,
  ZStreamExtension
}
import zio.{durationInt, Console, ZIO}

/**
 * Deploy a simple application mode job.
 */
@main def deploySimpleApplicationJob = unsafeRun {

  val spec = FlinkDeploymentDef(
    name = "simple-appjob",
    namespace = "fdev",
    image = "flink:1.16",
    flinkVersion = FlinkVersion.v1_16,
    jobManager = JobManagerDef(cpu = 1, memory = "1024m"),
    taskManager = TaskManagerDef(cpu = 1, memory = "1024m"),
    job = JobDef(
      jarURI = "local:///opt/flink/examples/streaming/StateMachineExample.jar",
      parallelism = 1
    )
  )
  for
    _ <- FileServer.launch
    _ <- FlinkK8sOperator.deployApplicationJob(114514, spec)
    _ <- FlinkK8sObserver.evaluatedJobSnaps.flatSubscribeValues().debugPretty.runDrain
  yield ()
}

/**
 * Deploy a simple flink cluster.
 */
@main def deploySimpleCluster = unsafeRun {

  val spec = FlinkDeploymentDef(
    name = "simple-session",
    namespace = "fdev",
    image = "flink:1.16",
    flinkVersion = FlinkVersion.v1_16,
    jobManager = JobManagerDef(cpu = 1, memory = "1024m"),
    taskManager = TaskManagerDef(cpu = 1, memory = "1024m")
  )
  for
    _ <- FileServer.launch
    _ <- FlinkK8sOperator.deployCluster(114515, spec)
    _ <- FlinkK8sObserver.clusterMetricsSnaps.flatSubscribeValues().debugPretty.runDrain
  yield ()
}

/**
 * Deploy a simple flink session mode job.
 */
@main def deploySimpleSessionJob = unsafeRun {

  val spec = FlinkSessionJobDef(
    namespace = "fdev",
    name = "simple-sessionjob",
    deploymentName = "simple-session",
    job = JobDef(
      jarURI = "assets/StateMachineExample.jar",
      parallelism = 1
    )
  )
  for
    _ <- FileServer.launch
    _ <- FlinkK8sOperator.deploySessionJob(114515, spec)
    _ <- FlinkK8sObserver.evaluatedJobSnaps.flatSubscribeValues().debugPretty("evaluated job status").runDrain.fork
    _ <- ZIO.never
  yield ()
}

/**
 * Deploy an application mode job with additional jar resources such as third-party dependencies pr udf.
 */
@main def deployApplicationJobWithExtraJars = unsafeRun {

  val spec = FlinkDeploymentDef(
    name = "appjob-with-extra-jar",
    namespace = "fdev",
    image = "flink:1.16",
    flinkVersion = FlinkVersion.v1_16,
    jobManager = JobManagerDef(cpu = 1, memory = "1024m"),
    taskManager = TaskManagerDef(cpu = 1, memory = "1024m"),
    job = JobDef(
      jarURI = "assets/quick-sql-1.0.jar",
      parallelism = 1,
      entryClass = "demo.flink.SqlFakerDataJob"
    ),
    extJarPaths = Array("assets/flink-faker-0.5.3.jar")
  )
  for
    _ <- FileServer.launch
    _ <- FlinkK8sOperator.deployApplicationJob(114514, spec)
    _ <- FlinkK8sObserver.evaluatedJobSnaps.flatSubscribeValues().debugPretty.runDrain
  yield ()
}

/**
 * Deploy an session mode job with additional jar resources such as third-party dependencies pr udf.
 */
@main def deployClusterAndSessionJarWithExtraJars = unsafeRun {

  val clusterSpec = FlinkDeploymentDef(
    name = "session-with-extra-jar",
    namespace = "fdev",
    image = "flink:1.16",
    flinkVersion = FlinkVersion.v1_16,
    jobManager = JobManagerDef(cpu = 1, memory = "1024m"),
    taskManager = TaskManagerDef(cpu = 1, memory = "1024m"),
    extJarPaths = Array("assets/flink-faker-0.5.3.jar")
  )
  val jobSpec     = FlinkSessionJobDef(
    namespace = "fdev",
    name = "sessionjob-with-extra-jar",
    deploymentName = "session-with-extra-jar",
    job = JobDef(
      jarURI = "assets/quick-sql-1.0.jar",
      parallelism = 1,
      entryClass = "demo.flink.SqlFakerDataJob"
    )
  )
  for
    _ <- FileServer.launch
    // deploy cluster
    _ <- FlinkK8sOperator.deployCluster(114514, clusterSpec)
    // deploy jar
    _ <- FlinkK8sOperator.deploySessionJob(114514, jobSpec)
    _ <- FlinkK8sObserver.evaluatedJobSnaps.flatSubscribeValues().debugPretty.runDrain
  yield ()
}

/**
 * Deploy an application job and set up ingress resources.
 */
@main def deployApplicationJobWithIngress = unsafeRun {

  val spec = FlinkDeploymentDef(
    name = "appjob-with-ingress",
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
  )
  for
    _ <- FileServer.launch
    _ <- FlinkK8sOperator.deployApplicationJob(114514, spec)
    _ <- FlinkK8sObserver.evaluatedJobSnaps.flatSubscribeValues().debugPretty.runDrain
  yield ()
}

/**
 * Cancel flink job.
 */
@main def cancelJob = unsafeRun {
  for
    _ <- FlinkK8sObserver.track(TrackKey.appJob(114514, "fdev", "simple-appjob"))
    _ <- FlinkK8sObserver.evaluatedJobSnaps
           .flatSubscribeValues()
           .takeUntil(snap => snap.clusterNs == "fdev" && snap.clusterId == "simple-appjob" && snap.jobStatus.nonEmpty)
           .runDrain

    _ <- Console.printLine("start to cancel job.")
    _ <- FlinkK8sOperator.cancelJob(114514)
    _ <- Console.printLine("job cancelled")
    _ <- ZIO.interrupt
  yield ()
}

/**
 * Stop the flink job and specify the corresponding savepoint configuration
 */
@main def stopJobWithSavepoint = unsafeRun {
  for
    _ <- FlinkK8sObserver.track(TrackKey.appJob(114514, "fdev", "simple-appjob"))
    _ <- FlinkK8sObserver.evaluatedJobSnaps
           .flatSubscribeValues()
           .takeUntil(snap => snap.clusterNs == "fdev" && snap.clusterId == "simple-appjob" && snap.jobStatus.nonEmpty)
           .runDrain

    _ <- Console.printLine("start to stop job.")
    _ <- FlinkK8sOperator
           .stopJob(114514, JobSavepointDef(savepointPath = "file:///opt/flink/savepoint"))
           .map(_.prettyStr)
           .debug("trigger status result")
    _ <- Console.printLine("job stopped.")
    _ <- ZIO.interrupt.ignore
  yield ()
}

/**
 * Trigger savepoint for flink job.
 */
@main def triggerJobSavepoint = unsafeRun {
  for
    _ <- FlinkK8sObserver.track(TrackKey.appJob(114514, "fdev", "simple-appjob"))
    _ <- FlinkK8sObserver.evaluatedJobSnaps
           .flatSubscribeValues()
           .takeUntil(snap => snap.clusterNs == "fdev" && snap.clusterId == "simple-appjob" && snap.jobStatus.nonEmpty)
           .runDrain

    _ <- Console.printLine("start to stop job.")
    _ <- FlinkK8sOperator
           .triggerJobSavepoint(114514, JobSavepointDef(savepointPath = "file:///opt/flink/savepoint"))
           .map(_.prettyStr)
           .debug("trigger status result")
    _ <- Console.printLine("job stopped.")
    _ <- ZIO.interrupt.ignore
  yield ()
}

/**
 * Delete flink cluster resources on kubernetes.
 */
@main def deleteCluster = unsafeRun {
  FlinkK8sOperator.k8sCrOpr.deleteDeployment("fdev", "simple-session")
}

/**
 * Delete flink application mode job resources on kubernetes.
 */
@main def deleteApplicationJob = unsafeRun {
  FlinkK8sOperator.k8sCrOpr.deleteDeployment("fdev", "simple-appjob")
}

/**
 * Delete flink session mode job resources on kubernetes.
 */
@main def deleteSessionJob = unsafeRun {
  FlinkK8sOperator.k8sCrOpr.deleteSessionJob("fdev", "simple-sessionjob")
}
