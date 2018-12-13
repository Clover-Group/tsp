package ru.itclover.tsp.v2.aggregators.accums

import cats.implicits._
import cats.{Functor, Monad}
import ru.itclover.tsp.core.{Time, Window}
import ru.itclover.tsp.io.TimeExtractor
import ru.itclover.tsp.io.TimeExtractor.GetTime
import ru.itclover.tsp.v2.Extract.IdxExtractor._
import ru.itclover.tsp.v2.Extract._
import ru.itclover.tsp.v2.QueueUtils.takeWhileFromQueue
import ru.itclover.tsp.v2._
import ru.itclover.tsp.v2.aggregators.AggregatorPatterns

import scala.Ordering.Implicits._
import scala.annotation.tailrec
import scala.collection.immutable.Queue
import scala.language.higherKinds

case class AggregatorPState[InnerState, AState <: AccumState[_, Out, AState], Out](
  innerState: InnerState,
  astate: AState,
  override val queue: QI[Out],
  indexTimeMap: Queue[(Idx, Time)]
) extends PState[Out, AggregatorPState[InnerState, AState, Out]] {
  override def copyWithQueue(queue: QI[Out]): AggregatorPState[InnerState, AState, Out] = this.copy(queue = queue)
}

abstract class AccumPattern[Event: IdxExtractor: TimeExtractor, Inner <: PState[InnerOut, Inner], InnerOut, Out, AState <: AccumState[
  InnerOut,
  Out,
  AState
], F[_]: Monad, Cont[_]: AddToQueue: Functor]
    extends AggregatorPatterns[Event, Out, AggregatorPState[Inner, AState, Out], F, Cont] {

  val innerPattern: Pattern[Event, InnerOut, Inner, F, Cont]
  val window: Window

  override def apply(
    state: AggregatorPState[Inner, AState, Out],
    event: Cont[Event]
  ): F[AggregatorPState[Inner, AState, Out]] = {

    val idxTimeMapWithNewEvents =
      AddToQueue[Cont].addToQueue(event.map(event => event.index -> event.time), state.indexTimeMap)

    innerPattern
      .apply(state.innerState, event)
      .map(
        newInnerState => {
          val (newInnerQueue, newAState, newResults, updatedIndexTimeMap) =
            processQueue(newInnerState, state.astate, idxTimeMapWithNewEvents)

          AggregatorPState(
            newInnerState.copyWithQueue(newInnerQueue),
            newAState,
            state.queue.enqueue(newResults),
            updatedIndexTimeMap
          )
        }
      )
  }

  private def processQueue(
    innerS: Inner,
    accumState: AState,
    indexTimeMap: Queue[(Idx, Time)]
  ): (QI[InnerOut], AState, QI[Out], Queue[(Idx, Time)]) = {

    @tailrec
    def innerFunc(
      innerQueue: QI[InnerOut],
      accumState: AState,
      collectedNewResults: QI[Out],
      indexTimeMap: Queue[(Idx, Time)]
    ): (QI[InnerOut], AState, QI[Out], Queue[(Idx, Time)]) =
      innerQueue.dequeueOption match {
        case None => (innerQueue, accumState, collectedNewResults, indexTimeMap)
        case Some((IdxValue(index, value), updatedQueue)) =>
          val (newInnerResultTime, updatedIdxTimeMap) = QueueUtils.rollMap(index, indexTimeMap)

          val (newAState, newResults) = accumState.updated(window, index, newInnerResultTime, value)

          innerFunc(
            updatedQueue,
            newAState,
            collectedNewResults.enqueue(newResults),
            updatedIdxTimeMap
          )
      }

    innerFunc(innerS.queue, accumState, Queue.empty, indexTimeMap)
  }

}

trait AccumState[In, Out, +Self <: AccumState[In, Out, Self]] extends Product with Serializable {

  def updated(window: Window, idx: Idx, time: Time, value: Result[In]): (Self, QI[Out])
}

/* Timer */
case class TimerPattern[Event: IdxExtractor: TimeExtractor, S <: PState[T, S], T, F[_]: Monad, Cont[_]: Functor: AddToQueue](
  override val innerPattern: Pattern[Event, T, S, F, Cont],
  override val window: Window
) extends AccumPattern[Event, S, T, T, TimerAccumState[T], F, Cont] {
  override def initialState(): AggregatorPState[S, TimerAccumState[T], T] = AggregatorPState(
    innerPattern.initialState(),
    astate = TimerAccumState(Queue.empty),
    queue = Queue.empty,
    indexTimeMap = Queue.empty
  )
}

case class TimerAccumState[T](windowQueue: Queue[(Idx, Time)]) extends AccumState[T, T, TimerAccumState[T]] {
  override def updated(window: Window, index: Idx, time: Time, value: Result[T]): (TimerAccumState[T], QI[T]) = {
    value match {
      // clean queue in case of fail. Return fails for all events in queue
      case Fail =>
        val (outputs, updatedWindowQueue) = takeWhileFromQueue(windowQueue)(_ => true)
        val newResults: QI[T] = outputs.map { case (idx, _) => IdxValue(idx, Result.fail[T]) }
        (TimerAccumState(updatedWindowQueue), newResults)
      // in case of Success we need to return Success for all events in window older than window size.
      case Succ(_) =>
        val (outputs, updatedWindowQueue) = takeWhileFromQueue(windowQueue) { case (_, t) => t.plus(window) < time }

        val windowQueueWithNewEvent = updatedWindowQueue.enqueue((index, time))
        val newResults: QI[T] = outputs.map { case (idx, _) => IdxValue(idx, value) }
        (TimerAccumState(windowQueueWithNewEvent), newResults)
    }
  }
}

/* Count */
/* WindowStatistic */

case class WindowStatistic[Event: IdxExtractor: TimeExtractor, S <: PState[T, S], T, F[_]: Monad, Cont[_]: Functor: AddToQueue](
  override val innerPattern: Pattern[Event, T, S, F, Cont],
  override val window: Window
) extends AccumPattern[Event, S, T, WindowStatisticResult, WindowStatisticAccumState[T], F, Cont] {
  override def initialState(): AggregatorPState[S, WindowStatisticAccumState[T], WindowStatisticResult] =
    AggregatorPState(
      innerState = innerPattern.initialState(),
      astate = WindowStatisticAccumState(None, Queue.empty),
      queue = Queue.empty,
      indexTimeMap = Queue.empty
    )
}

case class WindowStatisticAccumState[T](
  lastValue: Option[WindowStatisticResult],
  windowQueue: Queue[WindowStatisticQueueInstance]
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
            time,
            value.isDefined,
            // count success and fail times by previous result, not current!
            successTimeFromPrevious = if (cmr.lastWasSuccess) time.toMillis - cmr.time.toMillis else 0,
            failTimeFromPrevious = if (!cmr.lastWasSuccess) time.toMillis - cmr.time.toMillis else 0
          )

          val newLV = WindowStatisticResult(
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
          WindowStatisticResult(time, value.isDefined, if (value.isDefined) 1 else 0, 0, if (value.isEmpty) 1 else 0, 0)
          -> WindowStatisticQueueInstance(
            time,
            isSuccess = value.isDefined,
            successTimeFromPrevious = 0,
            failTimeFromPrevious = 0
          )
        )

    //remove outdated elements from queue
    val (outputs, updatedWindowQueue) = takeWhileFromQueue(windowQueue)(_.time.plus(window) < time)

    val finalNewLastValue = outputs.foldLeft(newLastValue) {
      case (cmr, elem) => {

        val maxChangeTime =
          lastValue.map(lv => window.toMillis - (lv.time.toMillis - elem.time.toMillis)).getOrElse(Long.MaxValue)

        if (elem.isSuccess) {
          WindowStatisticResult(
            time,
            lastWasSuccess = value.isDefined,
            successCount = cmr.successCount - 1,
            successMillis = cmr.successMillis - math.min(maxChangeTime, elem.successTimeFromPrevious),
            failCount = cmr.failCount,
            failMillis = cmr.failMillis - math.min(maxChangeTime, elem.failTimeFromPrevious)
          )
        } else {
          WindowStatisticResult(
            time,
            lastWasSuccess = value.isDefined,
            successCount = cmr.successCount,
            successMillis = cmr.successMillis - math.min(maxChangeTime, elem.successTimeFromPrevious),
            failCount = cmr.failCount - 1,
            failMillis = cmr.failMillis - math.min(maxChangeTime, elem.failTimeFromPrevious)
          )
        }
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

    val finalWindowQueue = updatedWindowQueue.enqueue(newWindowStatisticQueueInstance)
    WindowStatisticAccumState(Some(correctedLastValue), finalWindowQueue) -> Queue(
      IdxValue(idx, Result.succ(correctedLastValue))
    )
  }

}

case class WindowStatisticQueueInstance(
  time: Time,
  isSuccess: Boolean,
  successTimeFromPrevious: Long,
  failTimeFromPrevious: Long
)

case class WindowStatisticResult(
  time: Time,
  lastWasSuccess: Boolean,
  successCount: Int,
  successMillis: Long,
  failCount: Int,
  failMillis: Long
)

/* Avg */
/* Lag */
