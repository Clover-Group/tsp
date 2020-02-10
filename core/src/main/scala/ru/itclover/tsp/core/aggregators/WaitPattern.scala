package ru.itclover.tsp.core.aggregators

import cats.Apply
import cats.instances.option._
import ru.itclover.tsp.core.Pattern.{Idx, IdxExtractor, QI}
import ru.itclover.tsp.core.QueueUtils.takeWhileFromQueue
import ru.itclover.tsp.core.io.TimeExtractor
import ru.itclover.tsp.core._

import scala.Ordering.Implicits._
import scala.collection.{mutable => m}

/* Wait pattern */
case class WaitPattern[Event: IdxExtractor: TimeExtractor, S, T](
  override val inner: Pattern[Event, S, T],
  override val window: Window
) extends AccumPattern[Event, S, T, T, WaitAccumState[T]] {
  override def initialState(): AggregatorPState[S, T, WaitAccumState[T]] = AggregatorPState(
    inner.initialState(),
    innerQueue = PQueue.empty,
    astate = WaitAccumState(m.Queue.empty),
    indexTimeMap = m.Queue.empty
  )

  override val patternTag: PatternTag = WaitPatternTag
}

case class WaitAccumState[T](windowQueue: m.Queue[(Idx, Time)], lastFail: Boolean = false)
    extends AccumState[T, T, WaitAccumState[T]] {

  /** This method is called for each IdxValue produced by inner patterns.
    *
    * @param window   - defines time window for accumulation.
    * @param times    - contains mapping Idx->Time for all events with Idx in [idxValue.start, idxValue.end].
    *                 Guaranteed to be non-empty.
    * @param idxValue - result from inner pattern.
    * @return Tuple of updated state and queue of results to be emitted from this pattern.
    */
  @inline
  override def updated(
    window: Window,
    times: m.Queue[(Idx, Time)],
    idxValue: IdxValue[T]
  ): (WaitAccumState[T], QI[T]) = {

    val start = if (lastFail) times.head._2.minus(window) else times.head._2
    val end = if (idxValue.value.isFail) times.last._2.minus(window) else times.last._2

    // don't use ++ here, slow!
    val windowQueueWithNewPoints = times.foldLeft(windowQueue) { case (a, b) => a.enqueue(b); a }

    val cleanedWindowQueue = windowQueueWithNewPoints.dropWhile {
      case (_, t) => t < start
    }

    val (outputs, updatedWindowQueue) = takeWhileFromQueue(cleanedWindowQueue) {
      case (_, t) => t <= end
    }

    val newOptResult = createIdxValue(outputs.headOption, outputs.lastOption, idxValue.value)

    (WaitAccumState(updatedWindowQueue, idxValue.value.isFail), newOptResult.map(PQueue.apply).getOrElse(PQueue.empty))

  }

  private def createIdxValue(
    optStart: Option[(Idx, Time)],
    optEnd: Option[(Idx, Time)],
    result: Result[T]
  ): Option[IdxValue[T]] =
    Apply[Option].map2(optStart, optEnd)((start, end) => IdxValue(start._1, end._1, result))
}
