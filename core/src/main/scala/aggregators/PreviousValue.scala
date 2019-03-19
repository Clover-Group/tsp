package ru.itclover.tsp.v2.aggregators

import ru.itclover.tsp.core.Time._
import ru.itclover.tsp.core.{Time, Window}
import ru.itclover.tsp.io.TimeExtractor
import ru.itclover.tsp.v2.Pattern._
import ru.itclover.tsp.v2._

import scala.Ordering.Implicits._
import scala.collection.{mutable => m}
import scala.language.higherKinds

case class PreviousValue[Event: IdxExtractor: TimeExtractor, State <: PState[Out, State], Out](
  override val innerPattern: Pattern[Event, State, Out],
  override val window: Window
) extends AccumPattern[Event, State, Out, Out, PreviousValueAccumState[Out]] {

  override def initialState() =
    AggregatorPState(
      innerPattern.initialState(),
      PreviousValueAccumState(m.Queue.empty, None),
      m.Queue.empty,
      m.Queue.empty
    )
}

case class PreviousValueAccumState[T](queue: QI[(Time, T)], lastTimeValue: Option[(Time, T)])
    extends AccumState[T, T, PreviousValueAccumState[T]] {
  override def updated(window: Window, idx: Idx, time: Time, value: Result[T]): (PreviousValueAccumState[T], QI[T]) = {
    // Timestamp which was actual to the (time - window) moment
    val actualQueue =
      queue.map(iv => iv.value.map(_._1)).collect { case Succ(v) => v }.filter(t => t.plus(window) <= time)
    val actualTs = if (actualQueue.nonEmpty) actualQueue.max else Time(Long.MinValue)
    val newIdxValue = value match {
      case Succ(t) => IdxValue(idx, Result.succ(time -> t))
      case Fail    => IdxValue(idx, Result.fail)
    }
    val newQueue = queue.filter(iv => iv.value.map(_._1 >= actualTs).getOrElse(false))
    newQueue.enqueue(newIdxValue)
    (
      PreviousValueAccumState(newQueue, this.lastTimeValue),
      newQueue
        .map(
          iv =>
            IdxValue[T](index = idx, value = iv.value.map(_._2))
              .asInstanceOf[IdxValue[T]] // for some reason it doesn't work without this line
        )
        .slice(0, 1)
    )

  }
}
