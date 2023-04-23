package org.apache.streampark.flink.kubernetes

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
