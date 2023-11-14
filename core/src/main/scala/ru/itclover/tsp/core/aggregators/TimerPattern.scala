package ru.itclover.tsp.core.aggregators

import ru.itclover.tsp.core.Pattern._
import ru.itclover.tsp.core.QueueUtils.takeWhileFromQueue
import ru.itclover.tsp.core.io.TimeExtractor
import ru.itclover.tsp.core.{Time, Window, _}
import cats.instances.option._
import cats.Apply
import ru.itclover.tsp.core.Time.MaxWindow

import scala.Ordering.Implicits._
import scala.collection.{mutable => m}
import com.typesafe.scalalogging.Logger

/* Timer */
case class TimerPattern[Event: IdxExtractor: TimeExtractor, S, T](
  override val inner: Pattern[Event, S, T],
  override val window: Window,
  val eventsMaxGapMs: Long
) extends AccumPattern[Event, S, T, Boolean, TimerAccumState[T]] {

  override def initialState(): AggregatorPState[S, T, TimerAccumState[T]] = AggregatorPState(
    inner.initialState(),
    innerQueue = PQueue.empty,
    astate = TimerAccumState(m.ArrayDeque.empty, (0L, Time(0L)), Fail, eventsMaxGapMs),
    indexTimeMap = m.ArrayDeque.empty
  )

}

case class TimerAccumState[T](
  windowQueue: m.ArrayDeque[(Idx, Time)],
  lastEnd: (Idx, Time),
  lastValue: Result[T],
  eventsMaxGapMs: Long
) extends AccumState[T, Boolean, TimerAccumState[T]] {

  val log = Logger(classOf[TimerAccumState[T]])

  @inline
  override def updated(
    window: Window,
    times: m.ArrayDeque[(Idx, Time)],
    idxValue: IdxValue[T]
  ): (TimerAccumState[T], QI[Boolean]) = {

    log.debug(s"Current state: $this")
    log.debug(s"Received event: $idxValue with times: $times")
    idxValue.value match {
      // clean queue in case of fail. Return fails for all arrived events
      // (unless the failing event occurs late enough to form the success window)
      case Fail =>
        val updatedWindowQueue = m.Queue.empty[(Idx, Time)]
        val queueToReturn = PQueue.empty[Boolean]
        val newOptResult = createIdxValue(
          times.headOption,
          times.lastOption,
          if (
            lastValue.isFail || (times.headOption
              .map(_._2.toMillis)
              .getOrElse(Long.MinValue) < lastEnd._2.toMillis + window.toMillis)
          ) {
            Fail
          } else {
            Succ(true)
          }
        )
        if (newOptResult.map(_.value.isSuccess).getOrElse(false) && idxValue.start < idxValue.end) {
          // If previous events are known to be success before a multi-line fail,
          // we need to return success only for the first line
          val actualNewResult = newOptResult.get
          val successPart = actualNewResult.copy(end = idxValue.start)
          val failPart = actualNewResult.copy(start = idxValue.start + 1, value = Fail)
          queueToReturn.enqueue(successPart, failPart)
          log.debug(s"Returning pair of results: $successPart and $failPart")
          log.debug(s"New state: ${TimerAccumState(updatedWindowQueue, times.last, idxValue.value, eventsMaxGapMs)}")
          (
            TimerAccumState(updatedWindowQueue, times.last, idxValue.value, eventsMaxGapMs),
            queueToReturn
          )
        } else {
          log.debug(s"Returning single result: $newOptResult")
          log.debug(s"New state: ${TimerAccumState(updatedWindowQueue, times.last, idxValue.value, eventsMaxGapMs)}")
          newOptResult.foreach(r => queueToReturn.enqueue(r))
          (
            TimerAccumState(updatedWindowQueue, times.last, idxValue.value, eventsMaxGapMs),
            queueToReturn
          )
        }
      // in case of Success we need to return Success for all events in window older than window size.
      case Succ(_) =>
        val start: Time = times.head._2
        val end: Time = times.last._2 // time corresponding to the idxValue.end

        // don't use ++ here, slow!
        val windowQueueWithNewPoints = times.foldLeft(windowQueue) { case (a, b) => a.append(b); a }

        // if the window is not full yet, return Fail on the earlier lines but keep them in the queue
        val (oldOutputs, cleanedWindowQueue) = takeWhileFromQueue(windowQueueWithNewPoints) { case (_, t) =>
          t < end.minus(window)
        }

        if (oldOutputs.isEmpty) {
          // window is not full yet, return Fail for all points but keep them in the queue
          val newOptResultFail = createIdxValue(times.headOption, times.lastOption, Result.fail)
          val newResults = newOptResultFail.map(PQueue.apply).getOrElse(PQueue.empty)
          log.debug(s"Returning results: $newResults")
          log.debug(s"New state: ${TimerAccumState(cleanedWindowQueue, times.last, idxValue.value, eventsMaxGapMs)}")
          (
            TimerAccumState(cleanedWindowQueue, times.last, idxValue.value, eventsMaxGapMs),
            newResults
          )
        } else {
          log.debug(s"Removing old data from queue: $oldOutputs")
          // return Success for the points late enough
          val startingPoint = oldOutputs.head._2.plus(window)
          val startingIdx = times.find(_._2 >= startingPoint).map(_._1).get // this will always be non-empty by def.
          val newResults = PQueue.empty[Boolean]
          if (startingIdx > times.head._1) {
            // Both fails and successes occur
            // (this may happen if the window completed in the middle of the arrived data)
            newResults.enqueue(IdxValue(times.head._1, startingIdx - 1, Fail))
          }
          // Successes should occur always, as least on the last row!
          newResults.enqueue(IdxValue(startingIdx, times.last._1, Succ(true)))
          log.debug(s"Returning results: $newResults")
          log.debug(s"New state: ${TimerAccumState(cleanedWindowQueue, times.last, idxValue.value, eventsMaxGapMs)}")
          (
            TimerAccumState(cleanedWindowQueue, times.last, idxValue.value, eventsMaxGapMs),
            newResults
          )
        }
    }
  }

  private def createIdxValue(
    optStart: Option[(Idx, Time)],
    optEnd: Option[(Idx, Time)],
    result: Result[Boolean]
  ): Option[IdxValue[Boolean]] =
    Apply[Option].map2(optStart, optEnd)((start, end) => IdxValue(start._1, end._1, result))

}
