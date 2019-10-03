package ru.itclover.tsp.core.aggregators

import cats.Group
import ru.itclover.tsp.core.Pattern.{QI, _}
import ru.itclover.tsp.core.QueueUtils.takeWhileFromQueue
import ru.itclover.tsp.core.Result._
import ru.itclover.tsp.core.io.TimeExtractor
import ru.itclover.tsp.core.{PState, Pattern, Time, Window, _}

import scala.Ordering.Implicits._
import scala.collection.{mutable => m}
import scala.language.higherKinds

//todo documentation
//todo tests
//todo simplify?
case class GroupPattern[Event: IdxExtractor: TimeExtractor, S <: PState[T, S], T: Group](
  override val inner: Pattern[Event, S, T],
  override val window: Window
) extends AccumPattern[Event, S, T, GroupAccumResult[T], GroupAccumState[T]] {

  val group: Group[T] = implicitly[Group[T]]

  override def initialState(): AggregatorPState[S, GroupAccumState[T], GroupAccumResult[T]] =
    AggregatorPState(
      innerState = inner.initialState(),
      astate = GroupAccumState(None, m.Queue.empty),
      queue = PQueue.empty,
      indexTimeMap = m.Queue.empty
    )
}

case class GroupAccumState[T: Group](lastValue: Option[GroupAccumResult[T]], windowQueue: m.Queue[GroupAccumValue[T]])
    extends AccumState[T, GroupAccumResult[T], GroupAccumState[T]] {

  override def updated(
    window: Window,
    times: m.Queue[(Idx, Time)],
    idxValue: IdxValue[T]
  ): (GroupAccumState[T], QI[GroupAccumResult[T]]) = {

    val (newLastValue, newWindowQueue, newOutputQueue) =
      times.foldLeft(Tuple3(lastValue, windowQueue, PQueue.empty[GroupAccumResult[T]])) {
        case ((lastValue, windowQueue, outputQueue), (idx, time)) =>
          addOnePoint(time, idx, window, idxValue.value, lastValue, windowQueue, outputQueue)
      }

    GroupAccumState(newLastValue, newWindowQueue) -> newOutputQueue
  }

  def addOnePoint(
    time: Time,
    idx: Idx,
    window: Window,
    value: Result[T],
    lastValue: Option[GroupAccumResult[T]],
    windowQueue: m.Queue[GroupAccumValue[T]],
    outputQueue: QI[GroupAccumResult[T]]
  ): (Option[GroupAccumResult[T]], m.Queue[GroupAccumValue[T]], QI[GroupAccumResult[T]]) = {
    value
      .map { t =>
        // add new element to queue
        val newLastValue = lastValue
          .map(cmr => GroupAccumResult(sum = Group[T].combine(cmr.sum, t), count = cmr.count + 1))
          .orElse(Option(GroupAccumResult(sum = t, count = 1)))

        //remove outdated elements from queue
        val (outputs, updatedWindowQueue) = takeWhileFromQueue(windowQueue)(_.time.plus(window) <= time)

        val finalNewLastValue = outputs.foldLeft(newLastValue) {
          case (cmr, elem) =>
            cmr.map(
              lastSum => GroupAccumResult(sum = Group[T].remove(lastSum.sum, elem.value), count = lastSum.count - 1)
            )
        }

        val finalWindowQueue = { updatedWindowQueue.enqueue(GroupAccumValue(idx, time, t)); updatedWindowQueue }

        Tuple3(
          finalNewLastValue,
          finalWindowQueue,
          outputQueue.enqueue(
            IdxValue(idx, idx, finalNewLastValue.toResult)
          )
        )
      }
      .getOrElse(Tuple3(lastValue, windowQueue, outputQueue))
  }

}

case class GroupAccumValue[T](idx: Idx, time: Time, value: T)

case class GroupAccumResult[T](sum: T, count: Long)
