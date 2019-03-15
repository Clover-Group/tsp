package ru.itclover.tsp.v2.aggregators

import ru.itclover.tsp.core.{Time, Window}
import ru.itclover.tsp.io.TimeExtractor
import ru.itclover.tsp.v2.Pattern._
import ru.itclover.tsp.v2.QueueUtils.takeWhileFromQueue
import ru.itclover.tsp.v2.{PState, Pattern, _}

import scala.Ordering.Implicits._
import scala.collection.{mutable => m}
import scala.language.higherKinds

// TOOD@fabura Docs? Rename?
case class WindowStatistic[Event: IdxExtractor: TimeExtractor, S <: PState[T, S], T](
  override val innerPattern: Pattern[Event, S, T],
  override val window: Window
) extends AccumPattern[Event, S, T, WindowStatisticResult, WindowStatisticAccumState[T]] {
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
            isSuccess = value.isSuccess,
            // count success and fail times by previous result, not current!
            successTimeFromPrevious = if (cmr.lastWasSuccess) time.toMillis - cmr.time.toMillis else 0,
            failTimeFromPrevious = if (!cmr.lastWasSuccess) time.toMillis - cmr.time.toMillis else 0
          )

          val newLV = WindowStatisticResult(
            idx = idx,
            time = time,
            lastWasSuccess = value.isSuccess,
            successCount = cmr.successCount + (if (value.isSuccess) 1 else 0),
            successMillis = cmr.successMillis + math.min(elem.successTimeFromPrevious, window.toMillis),
            failCount = cmr.failCount + (if (value.isFail) 1 else 0),
            failMillis = cmr.failMillis + math.min(elem.failTimeFromPrevious, window.toMillis)
          )

          newLV -> elem
        }
        }
        .getOrElse(
          WindowStatisticResult(
            idx,
            time,
            value.isSuccess,
            if (value.isSuccess) 1 else 0,
            0,
            if (value.isFail) 1 else 0,
            0
          )
          -> WindowStatisticQueueInstance(
            idx,
            time,
            isSuccess = value.isSuccess,
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
            lastWasSuccess = value.isSuccess,
            successCount = cmr.successCount - 1,
            successMillis = cmr.successMillis - math.min(maxChangeTime, elem.successTimeFromPrevious),
            failCount = cmr.failCount,
            failMillis = cmr.failMillis - math.min(maxChangeTime, elem.failTimeFromPrevious)
          )
        } else {
          WindowStatisticResult(
            idx,
            time,
            lastWasSuccess = value.isSuccess,
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

// OPTIMIZE memory, make successCount and failCount - Int
case class WindowStatisticResult(
  idx: Idx,
  time: Time,
  lastWasSuccess: Boolean,
  successCount: Long,
  successMillis: Long,
  failCount: Long,
  failMillis: Long
) {
  def totalMillis: Idx = successMillis + failMillis
  def totalCount: Idx = successCount + failCount
}
