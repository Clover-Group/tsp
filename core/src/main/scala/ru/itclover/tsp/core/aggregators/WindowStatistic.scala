package ru.itclover.tsp.core.aggregators

import ru.itclover.tsp.core.Pattern._
import ru.itclover.tsp.core.QueueUtils.takeWhileFromQueue
import ru.itclover.tsp.core.io.TimeExtractor
import ru.itclover.tsp.core.{PState, Pattern, Time, Window, _}

import scala.Ordering.Implicits._
import scala.collection.{ mutable => m}
import scala.language.higherKinds

//todo docs
//todo simplify
case class WindowStatistic[Event: IdxExtractor: TimeExtractor, S <: PState[T, S], T](
  override val inner: Pattern[Event, S, T],
  override val window: Window
) extends AccumPattern[Event, S, T, WindowStatisticResult, WindowStatisticAccumState[T]] {
  override def initialState(): AggregatorPState[S, WindowStatisticAccumState[T], WindowStatisticResult] =
    AggregatorPState(
      innerState = inner.initialState(),
      astate = WindowStatisticAccumState(None, m.Queue.empty),
      queue = PQueue.empty,
      indexTimeMap = m.Queue.empty
    )
}

case class WindowStatisticAccumState[T](
  lastValue: Option[WindowStatisticResult],
  windowQueue: m.Queue[WindowStatisticQueueInstance]
) extends AccumState[T, WindowStatisticResult, WindowStatisticAccumState[T]] {
  override def updated(
    window: Window,
    times: m.Queue[(Idx, Time)],
    idxValue: IdxValue[T]
  ): (WindowStatisticAccumState[T], QI[WindowStatisticResult]) = {
    val isSuccess = idxValue.value.isSuccess
    val (newLastValue, newWindowQueue, newOutputQueue) =
      times.foldLeft(Tuple3(lastValue, windowQueue, PQueue.empty[WindowStatisticResult])) {
        case ((lastValue, windowQueue, outputQueue), (idx, time)) =>
          addOnePoint(time, idx, window, isSuccess, lastValue, windowQueue, outputQueue)
      }

    WindowStatisticAccumState[T](newLastValue, newWindowQueue) -> newOutputQueue
  }

  def addOnePoint(
    time: Time,
    idx: Idx,
    window: Window,
    isSuccess: Boolean,
    lastValue: Option[WindowStatisticResult],
    windowQueue: m.Queue[WindowStatisticQueueInstance],
    outputQueue: QI[WindowStatisticResult]
  ): (Option[WindowStatisticResult], m.Queue[WindowStatisticQueueInstance], QI[WindowStatisticResult]) = {

    // add new element to queue
    val (newLastValue, newWindowStatisticQueueInstance) =
      lastValue
        .map { cmr =>
          {

            val elem = WindowStatisticQueueInstance(
              idx = idx,
              time = time,
              isSuccess = isSuccess,
              // count success and fail times by previous result, not current!
              successTimeFromPrevious = if (cmr.lastWasSuccess) time.toMillis - cmr.time.toMillis else 0,
              failTimeFromPrevious = if (!cmr.lastWasSuccess) time.toMillis - cmr.time.toMillis else 0
            )

            val newLV = cmr.plusChange(elem, window)

            newLV -> elem
          }
        }
        .getOrElse(
          WindowStatisticResult(idx, time, isSuccess, if (isSuccess) 1 else 0, 0, if (!isSuccess) 1 else 0, 0)
          -> WindowStatisticQueueInstance(idx, time, isSuccess = isSuccess)
        )

    //remove outdated elements from queue
    val (outputs, updatedWindowQueue) = takeWhileFromQueue(windowQueue)(_.time.plus(window) < time)

    val finalNewLastValue = outputs.foldLeft(newLastValue) { case (cmr, elem) => cmr.minusChange(elem, window) }

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
    val updatedOutputQueue = outputQueue.enqueue(IdxValue(idx, idx, Result.succ(correctedLastValue)))

    Tuple3(Some(correctedLastValue), finalWindowQueue, updatedOutputQueue)
  }

}

case class WindowStatisticQueueInstance(
  idx: Idx,
  time: Time,
  isSuccess: Boolean,
  successTimeFromPrevious: Long = 0,
  failTimeFromPrevious: Long = 0
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

  def plusChange(wsqi: WindowStatisticQueueInstance, window: Window): WindowStatisticResult = WindowStatisticResult(
    idx = idx,
    time = time,
    lastWasSuccess = lastWasSuccess,
    successCount = successCount + (if (wsqi.isSuccess) 1 else 0),
    successMillis = successMillis + math.min(wsqi.successTimeFromPrevious, window.toMillis),
    failCount = failCount + (if (!wsqi.isSuccess) 1 else 0),
    failMillis = failMillis + math.min(wsqi.failTimeFromPrevious, window.toMillis)
  )

  def minusChange(wsqi: WindowStatisticQueueInstance, window: Window): WindowStatisticResult = {
    val maxChangeTime = window.toMillis - (time.toMillis - wsqi.time.toMillis)

    if (wsqi.isSuccess) {
      WindowStatisticResult(
        idx,
        time,
        lastWasSuccess = lastWasSuccess,
        successCount = successCount - 1,
        successMillis = successMillis - math.min(maxChangeTime, wsqi.successTimeFromPrevious),
        failCount = failCount,
        failMillis = failMillis - math.min(maxChangeTime, wsqi.failTimeFromPrevious)
      )
    } else {
      WindowStatisticResult(
        idx,
        time,
        lastWasSuccess = lastWasSuccess,
        successCount = successCount,
        successMillis = successMillis - math.min(maxChangeTime, wsqi.successTimeFromPrevious),
        failCount = failCount - 1,
        failMillis = failMillis - math.min(maxChangeTime, wsqi.failTimeFromPrevious)
      )
    }
  }

}
