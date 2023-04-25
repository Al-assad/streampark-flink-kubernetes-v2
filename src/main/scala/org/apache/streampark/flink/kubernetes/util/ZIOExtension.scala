package org.apache.streampark.flink.kubernetes.util

import zio.stream.{Stream, UStream, ZStream}
import zio.*
import zio.logging.backend.SLF4J

@throws[Throwable]
inline def unsafeRun[E, A](zio: IO[E, A]): A = Unsafe.unsafe { implicit u =>
  Runtime.default.unsafe
    .run(zio.provideLayer(Runtime.removeDefaultLoggers >>> SLF4J.slf4j))
    .getOrThrowFiberFailure()
}

extension [R, E, A](stream: ZStream[R, E, A])
  inline def diff: ZStream[R, E, A] = stream.zipWithPrevious
    .filter((prev, cur) => !prev.contains(cur))
    .map((_, cur) => cur)

extension [E, A](io: IO[E, A])
  @throws[Throwable]
  inline def runIO: A = unsafeRun(io)

extension [A](uio: UIO[A]) {
  inline def runUIO: A = unsafeRun(uio)
}

implicit class ZIOExtension[R, E, A](zio: ZIO[R, E, A]) {

  inline def debugPretty: ZIO[R, E, A] =
    zio
      .tap(value => ZIO.succeed(println(toPrettyString(value))))
      .tapErrorCause { cause => ZIO.succeed(println(s"<FAIL> ${cause.prettyPrint}")) }

  inline def debugPretty(tag: String): ZIO[R, E, A] =
    zio
      .tap(value => ZIO.succeed(println(s"$tag: ${toPrettyString(value)}")))
      .tapErrorCause { cause => ZIO.succeed(println(s"<FAIL> $tag: ${cause.prettyPrint}")) }
}

implicit class ZStreamExtension[R, E, A](zstream: ZStream[R, E, A]) {

  inline def debugPretty: ZStream[R, E, A] =
    zstream
      .tap(value => ZIO.succeed(println(toPrettyString(value))))
      .tapErrorCause { cause => ZIO.succeed(println(s"<FAIL> ${cause.prettyPrint}")) }

  inline def debugPretty(tag: String): ZStream[R, E, A] =
    zstream
      .tap(value => ZIO.succeed(println(s"$tag: ${toPrettyString(value)}")))
      .tapErrorCause { cause => ZIO.succeed(println(s"<FAIL> $tag: ${cause.prettyPrint}")) }
}
