package org.apache.streampark.flink.kubernetes

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.streampark.flink.kubernetes.FlinkRestRequest.*
import org.apache.streampark.flink.kubernetes.model.{FlinkPipeOprStates, JobSavepointDef, JobSavepointStatus}
import sttp.client3.*
import sttp.client3.httpclient.zio.HttpClientZioBackend
import sttp.client3.ziojson.*
import zio.json.{jsonField, JsonCodec}
import zio.{IO, Task, ZIO}

import scala.util.chaining.scalaUtilChainingOps

type TriggerId = String

/**
 * Flink rest-api request.
 * copy from [[https://github.com/Al-assad/potamoi/blob/master/potamoi-flink/src/main/scala/potamoi/flink/FlinkRestRequest.scala]]
 */
case class FlinkRestRequest(restUrl: String) {

  /**
   * Get all job overview info
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jobs-overview
   */
  def listJobOverviewInfo: IO[Throwable, Vector[JobOverviewInfo]] = usingSttp { backend =>
    request
      .get(uri"$restUrl/jobs/overview")
      .response(asJson[JobOverviewRsp])
      .send(backend)
      .flattenBodyT
      .map(_.jobs)
  }

  /**
   * Get cluster overview
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#overview-1
   */
  def getClusterOverview: IO[Throwable, ClusterOverviewInfo] = usingSttp { backend =>
    request
      .get(uri"$restUrl/overview")
      .response(asJson[ClusterOverviewInfo])
      .send(backend)
      .flattenBodyT
  }

  /**
   * Get job manager configuration.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jobmanager-config
   */
  def getJobmanagerConfig: IO[Throwable, Map[String, String]] = usingSttp { backend =>
    request
      .get(uri"$restUrl/jobmanager/config")
      .send(backend)
      .flattenBody
      .attemptBody(ujson.read(_).arr.map(item => item("key").str -> item("value").str).toMap)
  }

  /**
   * Cancels job.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jobs-jobid-1
   */
  def cancelJob(jobId: String): IO[Throwable, Unit] =
    usingSttp { backend =>
      request
        .patch(uri"$restUrl/jobs/$jobId?mode=cancel")
        .send(backend)
        .unit
    }

  /**
   * Stops job with savepoint.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jobs-jobid-stop
   */
  def stopJobWithSavepoint(jobId: String, sptReq: StopJobSptReq): IO[Throwable, TriggerId] =
    usingSttp { backend =>
      request
        .post(uri"$restUrl/jobs/$jobId/stop")
        .body(sptReq)
        .send(backend)
        .flattenBody
        .attemptBody(ujson.read(_)("request-id").str)
    }

  /**
   * Triggers a savepoint of job.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jobs-jobid-savepoints
   */
  def triggerSavepoint(jobId: String, sptReq: TriggerSptReq): IO[Throwable, TriggerId] =
    usingSttp { backend =>
      request
        .post(uri"$restUrl/jobs/$jobId/savepoints")
        .body(sptReq)
        .send(backend)
        .flattenBody
        .attemptBody(ujson.read(_)("request-id").str)
    }

  /**
   * Get status of savepoint operation.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jobs-jobid-savepoints-triggerid
   */
  def getSavepointOperationStatus(jobId: String, triggerId: String): IO[Throwable, JobSavepointStatus] =
    usingSttp { backend =>
      request
        .get(uri"$restUrl/jobs/$jobId/savepoints/$triggerId")
        .send(backend)
        .flattenBody
        .attemptBody { body =>
          val rspJson                  = ujson.read(body)
          val status                   = rspJson("status")("id").str.pipe(FlinkPipeOprStates.ofRaw)
          val (location, failureCause) = rspJson("operation").objOpt match
            case None            => None -> None
            case Some(operation) =>
              println(operation)
              val loc     = operation.get("location").flatMap(_.strOpt)
              val failure = operation.get("failure-cause").flatMap(_.objOpt.flatMap(_.get("stack-trace").strOpt))
              loc -> failure
          JobSavepointStatus(status, failureCause, location)
        }
    }
}

object FlinkRestRequest {

  val request = basicRequest

  def usingSttp[A](request: SttpBackend[Task, Any] => IO[Throwable, A]): IO[Throwable, A] =
    ZIO.scoped {
      HttpClientZioBackend.scoped().flatMap(backend => request(backend))
    }

  extension [A](requestIO: Task[Response[Either[ResponseException[String, String], A]]]) {
    inline def flattenBodyT: IO[Throwable, A] = requestIO.flatMap(rsp => ZIO.fromEither(rsp.body))
  }

  extension (requestIO: Task[Response[Either[String, String]]]) {
    inline def flattenBody: IO[Throwable, String] = requestIO.flatMap(rsp => ZIO.fromEither(rsp.body).mapError(new Exception(_)))
  }

  extension [A](requestIO: Task[String])
    inline def attemptBody(f: String => A): IO[Throwable, A] =
      requestIO.flatMap { body => ZIO.attempt(f(body)) }

  // Flink rest api models

  case class JobOverviewRsp(jobs: Vector[JobOverviewInfo]) derives JsonCodec

  case class JobOverviewInfo(
      @jsonField("jid") jid: String,
      name: String,
      state: String,
      @jsonField("start-time") startTime: Long,
      @jsonField("end-time") endTime: Long,
      @jsonField("last-modification") lastModifyTime: Long,
      tasks: TaskStats)
      derives JsonCodec

  case class TaskStats(
      total: Int,
      created: Int,
      scheduled: Int,
      deploying: Int,
      running: Int,
      finished: Int,
      canceling: Int,
      canceled: Int,
      failed: Int,
      reconciling: Int,
      initializing: Int)
      derives JsonCodec

  case class ClusterOverviewInfo(
      @jsonField("flink-version") flinkVersion: String,
      @jsonField("taskmanagers") taskManagers: Int,
      @jsonField("slots-total") slotsTotal: Int,
      @jsonField("slots-available") slotsAvailable: Int,
      @jsonField("jobs-running") jobsRunning: Int,
      @jsonField("jobs-finished") jobsFinished: Int,
      @jsonField("jobs-cancelled") jobsCancelled: Int,
      @jsonField("jobs-failed") jobsFailed: Int)
      derives JsonCodec

  case class StopJobSptReq(
      drain: Boolean = false,
      formatType: Option[String] = None,
      targetDirectory: Option[String],
      triggerId: Option[String] = None)
      derives JsonCodec

  object StopJobSptReq:
    def apply(sptConf: JobSavepointDef): StopJobSptReq =
      StopJobSptReq(sptConf.drain, sptConf.formatType, sptConf.savepointPath, sptConf.triggerId)

  case class TriggerSptReq(
      @jsonField("cancel-job") cancelJob: Boolean = false,
      formatType: Option[String] = None,
      @jsonField("target-directory") targetDirectory: Option[String],
      triggerId: Option[String] = None)
      derives JsonCodec
  
  object TriggerSptReq:
    def apply(sptConf: JobSavepointDef): TriggerSptReq =
      TriggerSptReq(cancelJob = false, sptConf.formatType, sptConf.savepointPath, sptConf.triggerId)
}
