package org.apache.streampark.flink.kubernetes

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator.Feature
import com.typesafe.scalalogging.Logger
import io.fabric8.kubernetes.client.dsl.{Resource, WatchAndWaitable}
import io.fabric8.kubernetes.client.*
import zio.{stream, *}
import zio.concurrent.{ConcurrentMap, ConcurrentSet}
import zio.stream.{Stream, UStream, ZStream}

import util.chaining.scalaUtilChainingOps

val jacksonMapper = ObjectMapper()

val yamlMapper = ObjectMapper(YAMLFactory().disable(Feature.WRITE_DOC_START_MARKER))
  .tap(_.setSerializationInclusion(JsonInclude.Include.NON_NULL))

/**
 * Syntax extension
 */
extension (value: AnyRef)
  def prettyStr: String                          = value match
    case v: String => v
    case v         => pprint.apply(value, height = 2000).render

/**
 * ZIO extension
 */
extension [E, A](io: IO[E, A]) inline def run: A = zioRun(io)

extension [A](uio: UIO[A]) inline def runNow: A = zioRun(uio)

extension [E, A](stream: Stream[E, A]) {

  inline def diff: Stream[E, A] = stream.zipWithPrevious
    .filter((prev, cur) => !prev.contains(cur))
    .map((_, cur) => cur)
}

inline def zioRun[E, A](zio: IO[E, A]) = Unsafe.unsafe { implicit u =>
  Runtime.default.unsafe.run(zio).getOrThrowFiberFailure()
}

/**
 * Subscription-ready data structure extension for ConcurrentSet
 */
implicit class ConcurrentSetExtension[E](set: ConcurrentSet[E]) {

  def sub(interval: Duration = 500.millis): UStream[Set[E]] =
    ZStream
      .fromZIO(set.toSet)
      .repeat(Schedule.spaced(interval))
      .diff

  def flatSub(interval: Duration = 500.millis): UStream[E] =
    ZStream
      .fromZIO(Ref.make(Set.empty[E]))
      .flatMap { prevSet =>
        sub(interval)
          .mapZIO(cur => prevSet.get.map(prev => (prev, cur)))
          .map((prev, cur) => cur -> cur.diff(prev))
          .tap((cur, _) => prevSet.set(cur))
          .flatMap((_, curDiff) => ZStream.fromIterable(curDiff))
      }
}

/**
 * Subscription-ready data structure extension for ConcurrentMap
 */
implicit class ConcurrentMapExtension[K, V](map: ConcurrentMap[K, V]) {

  def sub(interval: Duration = 500.millis): UStream[Chunk[(K, V)]] =
    ZStream
      .fromZIO(map.toChunk)
      .repeat(Schedule.spaced(interval))
      .diff

  // noinspection DuplicatedCode
  def flatSub(interval: Duration = 500.millis) =
    ZStream
      .fromZIO(Ref.make(Chunk.empty[(K, V)]))
      .flatMap { prevMap =>
        sub(interval)
          .mapZIO(cur => prevMap.get.map(prev => (prev, cur)))
          .map((prev, cur) => cur -> cur.diff(prev))
          .tap((cur, _) => prevMap.set(cur))
          .flatMap((_, curDiff) => ZStream.fromIterable(curDiff))
      }

  def subValues(interval: Duration = 500.millis): UStream[Chunk[V]] =
    sub(interval).map(_.map(_._2))

  def flatSubValues(interval: Duration = 500.millis): UStream[V] =
    flatSub(interval).map(_._2)
}

/**
 * Subscription-ready data structure extension for Ref[Map]
 */
implicit class RefMapExtension[K, V](ref: Ref[Map[K, V]]) {

  def sub(interval: Duration = 500.millis): UStream[Chunk[(K, V)]] =
    ZStream
      .fromZIO(ref.get.map(m => Chunk.fromIterable(m)))
      .repeat(Schedule.spaced(interval))
      .diff

  // noinspection DuplicatedCode
  def flatSub(interval: Duration = 500.millis) =
    ZStream
      .fromZIO(Ref.make(Chunk.empty[(K, V)]))
      .flatMap { prevMap =>
        sub(interval)
          .mapZIO(cur => prevMap.get.map(prev => (prev, cur)))
          .map((prev, cur) => cur -> cur.diff(prev))
          .tap((cur, _) => prevMap.set(cur))
          .flatMap((_, curDiff) => ZStream.fromIterable(curDiff))
      }

  def subValues(interval: Duration = 500.millis): UStream[Chunk[V]] =
    sub(interval).map(_.map(_._2))

  def flatSubValues(interval: Duration = 500.millis): UStream[V] =
    flatSub(interval).map(_._2)
}
