package ru.itclover.tsp.core.aggregators

import ru.itclover.tsp.core.Pattern._
import ru.itclover.tsp.core.Time._
import ru.itclover.tsp.core.io.TimeExtractor
import ru.itclover.tsp.core.{Time, Window, _}

import scala.Ordering.Implicits._
import scala.annotation.tailrec
import scala.collection.{mutable => m}

case class PreviousValue[Event: IdxExtractor: TimeExtractor, State, Out](
  override val inner: Pattern[Event, State, Out],
  override val window: Window
) extends AccumPattern[Event, State, Out, Out, PreviousValueAccumState[Out]] {

  override def initialState() =
    AggregatorPState(
      inner.initialState(),
      PQueue.empty,
      PreviousValueAccumState(PQueue.empty),
      m.Queue.empty
    )
}

// todo simplify
case class PreviousValueAccumState[T](queue: QI[(Time, T)]) extends AccumState[T, T, PreviousValueAccumState[T]] {
  override def updated(
    window: Window,
    times: m.ArrayDeque[(Idx, Time)],
    idxValue: IdxValue[T]
  ): (PreviousValueAccumState[T], QI[T]) = {
    val (newQueue, newOutputQueue) =
      times.foldLeft(queue -> PQueue.empty[T]) {
        case ((q, output), (idx, time)) =>
          addOnePoint(time, idx, window, idxValue.value, q, output)
      }

    PreviousValueAccumState(newQueue) -> newOutputQueue
  }

  private def addOnePoint(
    time: Time,
    idx: Idx,
    window: Window,
    value: Result[T],
    queue: QI[(Time, T)],
    output: QI[T]
  ): (QI[(Time, T)], QI[T]) = {
    // Timestamp and value which was actual to the (time - window) moment
    def splitAtActualTs(): (Option[T], QI[(Time, T)]) = {
      @tailrec
      def inner(q: QI[(Time, T)], v: Option[T]): (Option[T], QI[(Time, T)]) = {

        q.headOption match {
          case Some(IdxValue(_, _, Succ((t, result)))) if t.plus(window) <= time => inner(q.behead(), Some(result))
          case Some(IdxValue(_, _, Fail))                                        => inner(q.behead(), v)
          case _                                                                 => (v, q)
        }
      }

      inner(queue, None)
    }

    val (newValue, newQueue) = splitAtActualTs()
    val newIdxValue = IdxValue(idx, idx, value.map(time -> _))
    //     we return first element after the moment time - window. Probably, better instead return _last_ element before moment time - window?
    val head = newValue // it's not probably, but certainly. Returning last before
    val updatedQueue = newQueue.enqueue(newIdxValue)

    updatedQueue ->
    head.map(x => IdxValue(idx, idx, Succ(x))).foldLeft(output) { case (q, x) => q.enqueue(x) }
  }
}
