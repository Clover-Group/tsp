package ru.itclover.tsp.core.aggregators

import ru.itclover.tsp.core.Pattern._
import ru.itclover.tsp.core.QueueUtils.takeWhileFromQueue
import ru.itclover.tsp.core.io.TimeExtractor
import ru.itclover.tsp.core.{Time, Window, _}
import cats.instances.option._
import cats.Apply

import scala.Ordering.Implicits._
import scala.collection.{mutable => m}
import scala.language.higherKinds

/* Timer */
case class TimerPattern[Event: IdxExtractor: TimeExtractor, S <: PState[T, S], T](
  override val inner: Pattern[Event, S, T],
  override val window: Window
) extends AccumPattern[Event, S, T, Unit, TimerAccumState[T]] {
  override def initialState(): AggregatorPState[S, TimerAccumState[T], Unit] = AggregatorPState(
    inner.initialState(),
    astate = TimerAccumState(m.Queue.empty),
    queue = PQueue.empty,
    indexTimeMap = m.Queue.empty
  )
}

case class TimerAccumState[T](windowQueue: m.Queue[(Idx, Time)]) extends AccumState[T, Unit, TimerAccumState[T]] {

  @inline
  override def updated(
    window: Window,
    times: m.Queue[(Idx, Time)],
    idxValue: IdxValue[T]
  ): (TimerAccumState[T], QI[Unit]) = {
    def createIdxValue(outputs: m.Queue[(Idx, Time)], result: Result[Unit]): Option[IdxValue[Unit]] =
      Apply[Option].map2(outputs.headOption, outputs.lastOption)((start, end) => IdxValue(start._1, end._1, Fail))

    val (updatedWindowQueue, newOptResult) = idxValue.value match {
      // clean queue in case of fail. Return fails for all events in queue
      case Fail => (m.Queue.empty[(Idx, Time)], createIdxValue(windowQueue, Fail))
      // in case of Success we need to return Success for all events in window older than window size.
      case Succ(_) =>
        val start: Time = times.head._2 // time corresponding to the idxValue.start
        val (outputs, updatedWindowQueue) = takeWhileFromQueue(windowQueue) { case (_, t) => t.plus(window) <= start }
        // add new times to windowQueue
        val windowQueueWithNewEvent = times.foldLeft(updatedWindowQueue)((q, timeIdx) => { q.enqueue(timeIdx); q })

        (windowQueueWithNewEvent, createIdxValue(outputs, Result.succUnit))
    }
    (TimerAccumState(updatedWindowQueue), newOptResult.map(PQueue.apply).getOrElse(PQueue.empty))
  }
}
