package ru.itclover.tsp.v2.aggregators

import cats.Group
import ru.itclover.tsp.core.{Time, Window}
import ru.itclover.tsp.io.TimeExtractor
import ru.itclover.tsp.v2.Pattern._
import ru.itclover.tsp.v2.QueueUtils.takeWhileFromQueue
import ru.itclover.tsp.v2.Result._
import ru.itclover.tsp.v2.{PState, Pattern, _}

import scala.Ordering.Implicits._
import scala.collection.{mutable => m}
import scala.language.higherKinds

case class GroupPattern[Event: IdxExtractor: TimeExtractor, S <: PState[T, S], T: Group](
  override val innerPattern: Pattern[Event, S, T],
  override val window: Window
) extends AccumPattern[Event, S, T, GroupAccumResult[T], GroupAccumState[T]] {
  override def initialState(): AggregatorPState[S, GroupAccumState[T], GroupAccumResult[T]] =
    AggregatorPState(
      innerState = innerPattern.initialState(),
      astate = GroupAccumState(None, m.Queue.empty),
      queue = m.Queue.empty,
      indexTimeMap = m.Queue.empty
    )
}

case class GroupAccumState[T: Group](lastValue: Option[GroupAccumResult[T]], windowQueue: m.Queue[GroupAccumValue[T]])
    extends AccumState[T, GroupAccumResult[T], GroupAccumState[T]] {
  override def updated(
    window: Window,
    idx: Idx,
    time: Time,
    value: Result[T]
  ): (GroupAccumState[T], QI[GroupAccumResult[T]]) = {
    value
      .map { t =>
        // add new element to queue
        val newLastValue = lastValue
          .map(cmr => GroupAccumResult(sum = Group[T].combine(cmr.sum, t), count = cmr.count + 1))
          .orElse(Option(GroupAccumResult(sum = t, count = 1)))

        //remove outdated elements from queue
        val (outputs, updatedWindowQueue) = takeWhileFromQueue(windowQueue)(_.time.plus(window) < time)

        val finalNewLastValue = outputs.foldLeft(newLastValue) {
          case (cmr, elem) =>
            cmr.map(
              lastSum => GroupAccumResult(sum = Group[T].remove(lastSum.sum, elem.value), count = lastSum.count - 1)
            )
        }

        val finalWindowQueue = { updatedWindowQueue.enqueue(GroupAccumValue(idx, time, t)); updatedWindowQueue }

        GroupAccumState(finalNewLastValue, finalWindowQueue) -> m.Queue(IdxValue(idx, finalNewLastValue.toResult))
      }
      .getOrElse(this -> m.Queue.empty)
  }

}

case class GroupAccumValue[T](idx: Idx, time: Time, value: T)

case class GroupAccumResult[T](sum: T, count: Long)
