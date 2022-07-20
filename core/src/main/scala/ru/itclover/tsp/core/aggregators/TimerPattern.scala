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

/* Timer */
case class TimerPattern[Event: IdxExtractor: TimeExtractor, S, T](
  override val inner: Pattern[Event, S, T],
  override val window: Window,
  val eventsMaxGapMs: Long
) extends AccumPattern[Event, S, T, Boolean, TimerAccumState[T]] {
  override def initialState(): AggregatorPState[S, T, TimerAccumState[T]] = AggregatorPState(
    inner.initialState(),
    innerQueue = PQueue.empty,
    astate = TimerAccumState(m.Queue.empty, (0L, Time(0L)), Fail, eventsMaxGapMs),
    indexTimeMap = m.Queue.empty
  )
}

case class TimerAccumState[T](
  windowQueue: m.ArrayDeque[(Idx, Time)],
  lastEnd: (Idx, Time),
  lastValue: Result[T],
  eventsMaxGapMs: Long
) extends AccumState[T, Boolean, TimerAccumState[T]] {

  @inline
  override def updated(
    window: Window,
    times: m.ArrayDeque[(Idx, Time)],
    idxValue: IdxValue[T]
  ): (TimerAccumState[T], QI[Boolean]) = {

    idxValue.value match {
      // clean queue in case of fail. Return fails for all events in queue
      // (unless the failing event occurs late enough to form the success window)
      // Do not return Fail for events before lastEnd, since they can be earlier reported as Success
      case Fail =>
        val updatedWindowQueue = m.Queue.empty[(Idx, Time)]
        val newOptResult = createIdxValue(
          windowQueue.dropWhile { case (i, _) => i <= lastEnd._1 }.headOption.orElse(times.headOption),
          times.lastOption,
          if (lastValue.isFail || (times.headOption
                .map(_._2.toMillis)
                .getOrElse(Long.MinValue) < lastEnd._2.toMillis + window.toMillis)) {
            Fail
          } else {
            Succ(true)
          }
        )
        (
          TimerAccumState(updatedWindowQueue, times.last, idxValue.value, eventsMaxGapMs),
          newOptResult.map(PQueue.apply).getOrElse(PQueue.empty)
        )
      // in case of Success we need to return Success for all events in window older than window size.
      case Succ(_) =>
        val start: Time = times.head._2.plus(window)
        val end: Time = times.last._2 // time corresponding to the idxValue.end

        // don't use ++ here, slow!
        val windowQueueWithNewPoints = times.foldLeft(windowQueue) { case (a, b) => a.append(b); a }

        // output fail on older points (before the end of the window)
        // but don't clean the whole queue
        def canOutput(t: Time): Boolean = {
          val last = windowQueue.lastOption.map(_._2).getOrElse(Time(Long.MaxValue))
          window != MaxWindow | t < last | Time(last.toMillis + eventsMaxGapMs) < start
        }

        val (failOutputs, cleanedWindowQueue) = takeWhileFromQueue(windowQueueWithNewPoints) {
          case (_, t) => t < start & canOutput(t)
        }

        val (outputs, updatedWindowQueue) = takeWhileFromQueue(cleanedWindowQueue) {
          case (_, t) => t.plus(window) <= end & canOutput(t)
        }

        // if event chunk is shorter than the window, and the next window is sufficiently close,
        // then save it in the queue, and return empty state (since the later events can continue the window)
        if (cleanedWindowQueue.isEmpty && times.head._2.toMillis - lastEnd._2.toMillis < eventsMaxGapMs) {
          updatedWindowQueue.appendAll(failOutputs)
          (
            TimerAccumState(updatedWindowQueue, times.last, idxValue.value, eventsMaxGapMs),
            PQueue.empty[Boolean]
          )
        } else {

          val newOptResultFail = createIdxValue(failOutputs.headOption, failOutputs.lastOption, Result.fail)
          val newOptResultSucc = createIdxValue(
            outputs.headOption.orElse(windowQueueWithNewPoints.headOption),
            Some((idxValue.end, end)),
            Result.succ(true)
          )
          val newResults = newOptResultSucc
            .map(newOptResultFail.map(PQueue.apply).getOrElse(PQueue.empty).enqueue(_))
            .getOrElse(PQueue.empty)
          (
            TimerAccumState(updatedWindowQueue, times.last, idxValue.value, eventsMaxGapMs),
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
