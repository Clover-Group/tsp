package ru.itclover.tsp.core.aggregators

import ru.itclover.tsp.core.{Time, Window}
import ru.itclover.tsp.core.IdxValue.{IdxValueSegment, IdxValueSimple}
import ru.itclover.tsp.core.PQueue._
import ru.itclover.tsp.core.Pattern._
import ru.itclover.tsp.core.QueueUtils.takeWhileFromQueue
import ru.itclover.tsp.core._
import ru.itclover.tsp.core.io.TimeExtractor

import scala.Ordering.Implicits._
import scala.collection.{mutable => m}

/* Timer */
case class TimerPattern[Event: IdxExtractor: TimeExtractor, S <: PState[T, S], T](
  override val inner: Pattern[Event, S, T],
  override val window: Window
) extends AccumPattern[Event, S, T, T, TimerAccumState[T]] {
  override def initialState(): AggregatorPState[S, TimerAccumState[T], T] = AggregatorPState(
    inner.initialState(),
    astate = TimerAccumState(m.Queue.empty),
    queue = PQueue.empty,
    indexTimeMap = m.Queue.empty
  )
}

case class TimerAccumState[T](windowQueue: m.Queue[(Idx, Time)]) extends AccumState[T, T, TimerAccumState[T]] {

  @inline
  override def updated(window: Window, index: Idx, time: Time, value: Result[T]): (TimerAccumState[T], QI[T]) = {
    value match {
      // clean queue in case of fail. Return fails for all events in queue
      case Fail =>
        val (outputs, updatedWindowQueue) = (windowQueue, m.Queue.empty[(Idx, Time)])
        val newResults: QI[T] = MutablePQueue(outputs.map { case (idx, _) => IdxValueSimple(idx, Result.fail[T]) })
        (TimerAccumState(updatedWindowQueue), newResults)
      // in case of Success we need to return Success for all events in window older than window size.
      case Succ(_) =>
        val (outputs, updatedWindowQueue) = takeWhileFromQueue(windowQueue) { case (_, t) => t.plus(window) <= time }

        val windowQueueWithNewEvent = { updatedWindowQueue.enqueue((index, time)); updatedWindowQueue }

        val newResults: QI[T] = MutablePQueue(outputs.map { case (idx, _) => IdxValueSegment(index, idx, index, value) })
        (TimerAccumState(windowQueueWithNewEvent), newResults)
    }
  }
}
