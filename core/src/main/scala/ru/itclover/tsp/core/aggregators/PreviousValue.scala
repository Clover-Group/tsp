package ru.itclover.tsp.core.aggregators

import ru.itclover.tsp.core.Time._
import ru.itclover.tsp.core.{Time, Window}
import ru.itclover.tsp.core.Pattern._
import ru.itclover.tsp.core._
import ru.itclover.tsp.core.io.TimeExtractor

import scala.Ordering.Implicits._
import scala.annotation.tailrec
import scala.collection.{mutable => m}
import scala.language.higherKinds

case class PreviousValue[Event: IdxExtractor: TimeExtractor, State <: PState[Out, State], Out](
  override val inner: Pattern[Event, State, Out],
  override val window: Window
) extends AccumPattern[Event, State, Out, Out, PreviousValueAccumState[Out]] {

  override def initialState() =
    AggregatorPState(
      inner.initialState(),
      PreviousValueAccumState(PQueue.empty),
      PQueue.empty,
      m.Queue.empty
    )
}

case class PreviousValueAccumState[T](queue: QI[(Time, T)]) extends AccumState[T, T, PreviousValueAccumState[T]] {
  override def updated(window: Window, idx: Idx, time: Time, value: Result[T]): (PreviousValueAccumState[T], QI[T]) = {
    // Timestamp which was actual to the (time - window) moment
    def splitAtActualTs(): (Time, QI[(Time, T)]) = {
      @tailrec
      def inner(prevBestTime: Time, q: QI[(Time, T)]): (Time, QI[(Time, T)]) = {
        q.headOption match {
          case Some(IdxValue(_, Succ((t, result)))) if t.plus(window) <= time => inner(t, q.behead())
          case Some(IdxValue(_, Fail))                                        => inner(prevBestTime, q.behead())
          case _                                                              => prevBestTime -> q
        }
      }

      inner(Time(Long.MinValue), queue)
    }

    val (actualTs, newQueue) = splitAtActualTs()
    val newIdxValue = IdxValue(idx, value.map(time -> _))
    // we return first element after the moment time - window. Probably, better instead return _last_ element before moment time - window?
    val head = newQueue.headOption
    val updatedQueue = newQueue.enqueue(newIdxValue)

    (
      PreviousValueAccumState(updatedQueue),
      head.map(_.map(x => Succ(x._2))).foldLeft(PQueue.empty[T]) { case (q, x) => q.enqueue(x) }
    )

  }
}
