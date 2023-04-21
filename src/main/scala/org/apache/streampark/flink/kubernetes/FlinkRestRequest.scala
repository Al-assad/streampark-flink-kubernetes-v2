package org.apache.streampark.flink.kubernetes

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.streampark.flink.kubernetes.FlinkRestRequest.*
import sttp.client3.httpclient.zio.HttpClientZioBackend
import sttp.client3.ziojson.asJson
import sttp.client3.*
import zio.json.{jsonField, JsonCodec}
import zio.{IO, Task, ZIO}

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
}
