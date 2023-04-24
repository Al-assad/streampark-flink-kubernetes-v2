package org.apache.streampark.flink.kubernetes.util

import zio.*
import zio.concurrent.{ConcurrentMap, ConcurrentSet}
import zio.stream.{UStream, ZStream}

/**
 * Subscription-ready data structure extension for ConcurrentSet
 */
implicit class ConcurrentSetExtension[E](set: ConcurrentSet[E]) {

  def sub(interval: Duration = 500.millis): UStream[Set[E]] =
    ZStream
      .fromZIO(set.toSet)
      .repeat(Schedule.spaced(interval))
      .changes

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
      .changes

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
      .changes

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

  def getValue(key: K): UIO[Option[V]] = ref.get.map(_.get(key))
}
