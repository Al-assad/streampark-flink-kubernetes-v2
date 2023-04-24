package org.apache.streampark.flink.kubernetes.util

import zio.stream.{Stream, UStream, ZStream}
import zio.*
import zio.logging.backend.SLF4J

@throws[Throwable]
def unsafeRun[E, A](zio: IO[E, A]): A = Unsafe.unsafe { implicit u =>
  Runtime.default.unsafe
    .run(zio.provideLayer(Runtime.removeDefaultLoggers >>> SLF4J.slf4j))
    .getOrThrowFiberFailure()
}

extension [E, A](io: IO[E, A])
  @throws[Throwable]
  inline def runIO: A = unsafeRun(io)

extension [A](uio: UIO[A]) {
  inline def runUIO: A = unsafeRun(uio)
}
