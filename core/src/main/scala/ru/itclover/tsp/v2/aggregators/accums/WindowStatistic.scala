package ru.itclover.tsp.v2.aggregators.accums

import cats.{Functor, Monad}
import ru.itclover.tsp.core.{Time, Window}
import ru.itclover.tsp.io.TimeExtractor
import ru.itclover.tsp.v2.Extract._
import ru.itclover.tsp.v2.QueueUtils.takeWhileFromQueue
import ru.itclover.tsp.v2.{AddToQueue, PState, Pattern, _}

import scala.Ordering.Implicits._
import scala.collection.{mutable => m}
import scala.language.higherKinds
/* Count */
/* WindowStatistic */

case class WindowStatistic[Event: IdxExtractor: TimeExtractor, S <: PState[T, S], T, F[_]: Monad, Cont[_]: Functor: AddToQueue](
  override val innerPattern: Pattern[Event, T, S, F, Cont],
  override val window: Window
) extends AccumPattern[Event, S, T, WindowStatisticResult, WindowStatisticAccumState[T], F, Cont] {
  override def initialState(): AggregatorPState[S, WindowStatisticAccumState[T], WindowStatisticResult] =
    AggregatorPState(
      innerState = innerPattern.initialState(),
      astate = WindowStatisticAccumState(None, m.Queue.empty),
      queue = m.Queue.empty,
      indexTimeMap = m.Queue.empty
    )
}

case class WindowStatisticAccumState[T](
  lastValue: Option[WindowStatisticResult],
  windowQueue: m.Queue[WindowStatisticQueueInstance]
) extends AccumState[T, WindowStatisticResult, WindowStatisticAccumState[T]] {
  override def updated(
    window: Window,
    idx: Idx,
    time: Time,
    value: Result[T]
  ): (WindowStatisticAccumState[T], QI[WindowStatisticResult]) = {

    // add new element to queue
    val (newLastValue, newWindowStatisticQueueInstance) =
      lastValue
        .map { cmr => {

          val elem = WindowStatisticQueueInstance(
            idx = idx,
            time = time,
            isSuccess = value.isDefined,
            // count success and fail times by previous result, not current!
            successTimeFromPrevious = if (cmr.lastWasSuccess) time.toMillis - cmr.time.toMillis else 0,
            failTimeFromPrevious = if (!cmr.lastWasSuccess) time.toMillis - cmr.time.toMillis else 0
          )

          val newLV = WindowStatisticResult(
            idx = idx,
            time = time,
            lastWasSuccess = value.isDefined,
            successCount = cmr.successCount + (if (value.isDefined) 1 else 0),
            successMillis = cmr.successMillis + math.min(elem.successTimeFromPrevious, window.toMillis),
            failCount = cmr.failCount + (if (value.isEmpty) 1 else 0),
            failMillis = cmr.failMillis + math.min(elem.failTimeFromPrevious, window.toMillis)
          )

          newLV -> elem
        }
        }
        .getOrElse(
          WindowStatisticResult(
            idx,
            time,
            value.isDefined,
            if (value.isDefined) 1 else 0,
            0,
            if (value.isEmpty) 1 else 0,
            0
          )
          -> WindowStatisticQueueInstance(
            idx,
            time,
            isSuccess = value.isDefined,
            successTimeFromPrevious = 0,
            failTimeFromPrevious = 0
          )
        )

    //remove outdated elements from queue
    val (outputs, updatedWindowQueue) = takeWhileFromQueue(windowQueue)(_.time.plus(window) < time)

    val finalNewLastValue = outputs.foldLeft(newLastValue) {
      case (cmr, elem) =>
        val maxChangeTime =
          lastValue.map(lv => window.toMillis - (lv.time.toMillis - elem.time.toMillis)).getOrElse(Long.MaxValue)

        if (elem.isSuccess) {
          WindowStatisticResult(
            idx,
            time,
            lastWasSuccess = value.isDefined,
            successCount = cmr.successCount - 1,
            successMillis = cmr.successMillis - math.min(maxChangeTime, elem.successTimeFromPrevious),
            failCount = cmr.failCount,
            failMillis = cmr.failMillis - math.min(maxChangeTime, elem.failTimeFromPrevious)
          )
        } else {
          WindowStatisticResult(
            idx,
            time,
            lastWasSuccess = value.isDefined,
            successCount = cmr.successCount,
            successMillis = cmr.successMillis - math.min(maxChangeTime, elem.successTimeFromPrevious),
            failCount = cmr.failCount - 1,
            failMillis = cmr.failMillis - math.min(maxChangeTime, elem.failTimeFromPrevious)
          )
        }
    }

    // we have to correct result because of the most early event in queue can contain additional time with is not in window.
    val correctedLastValue = updatedWindowQueue.headOption
      .map { cmqi =>
        val maxChangeTime = window.toMillis - (finalNewLastValue.time.toMillis - cmqi.time.toMillis)
        val successCorrection =
          if (cmqi.successTimeFromPrevious == 0) 0 else cmqi.successTimeFromPrevious - maxChangeTime
        val failCorrection = if (cmqi.failTimeFromPrevious == 0) 0 else cmqi.failTimeFromPrevious - maxChangeTime
        finalNewLastValue.copy(
          successMillis = finalNewLastValue.successMillis - successCorrection,
          failMillis = finalNewLastValue.failMillis - failCorrection
        )
      }
      .getOrElse(finalNewLastValue)

    val finalWindowQueue = { updatedWindowQueue.enqueue(newWindowStatisticQueueInstance); updatedWindowQueue }
    WindowStatisticAccumState(Some(correctedLastValue), finalWindowQueue) -> m.Queue(
      IdxValue(idx, Result.succ(correctedLastValue))
    )
  }

}

case class WindowStatisticQueueInstance(
  idx: Idx,
  time: Time,
  isSuccess: Boolean,
  successTimeFromPrevious: Long,
  failTimeFromPrevious: Long
)

case class WindowStatisticResult(
  idx: Idx,
  time: Time,
  lastWasSuccess: Boolean,
  successCount: Int,
  successMillis: Long,
  failCount: Int,
  failMillis: Long
)