package ru.itclover.streammachine.aggregators

import scala.Ordering.Implicits._
import scala.math.Numeric.Implicits._
import ru.itclover.streammachine.core.{PhaseParser, PhaseResult, Time, Window}
import ru.itclover.streammachine.core.PhaseResult.{Failure, Stay, Success}
import ru.itclover.streammachine.core.Time.TimeExtractor


case class AccumulationPhase[Event, InnerState, AccumOut, Out]
    (innerPhase: PhaseParser[Event, InnerState, AccumOut], window: Window, accumulator: () => AccumulatedState[AccumOut])
    (extractResult: AccumulatedState[AccumOut] => Out, extractorName: String)
    (implicit timeExtractor: TimeExtractor[Event])
  extends AggregatorPhases[Event, (InnerState, AccumulatedState[AccumOut]), Out] {

  override def apply(event: Event, oldState: (InnerState, AccumulatedState[AccumOut])):
    (PhaseResult[Out], (InnerState, AccumulatedState[AccumOut])) =
  {
    val time = timeExtractor(event)
    val (oldInnerState, oldAccumState) = oldState
    val (innerResult, newInnerState) = innerPhase(event, oldInnerState)



    innerResult match {
      case Success(t) => {
        val newAccumState = oldAccumState.updated(time, t)

        val newAccumResult = newAccumState.startTime match {
          // Success, if window is fully accumulated (all window time has passed)
          case Some(startTime) if time >= startTime.plus(window) => Success(extractResult(newAccumState))
          case _ => Stay
        }

        newAccumResult -> (newInnerState -> newAccumState)
      }
      case f@Failure(msg) =>
        Failure(msg) -> (newInnerState -> oldAccumState) // aggregate here?
      case Stay => Stay ->
        (newInnerState -> oldAccumState)
    }
  }

  override def initialState: (InnerState, AccumulatedState[AccumOut]) = innerPhase.initialState -> accumulator()

  override def format(event: Event, state: (InnerState, AccumulatedState[AccumOut])) = if (state._2.hasState) {
    s"$extractorName(${innerPhase.format(event, state._1)})=${extractResult(state._2)}"
  } else {
    s"$extractorName(${innerPhase.format(event, state._1)})"
  }
}

object AccumulationPhase {
  def apply[Event, InnerState, AccumOut, Out]
      (inner: PhaseParser[Event, InnerState, AccumOut], accumulator: => AccumulatedState[AccumOut], window: Window)
      (extractResult: AccumulatedState[AccumOut] => Out, extractorName: String)
      (implicit timeExtractor: TimeExtractor[Event]) =
    new AccumulationPhase(inner, window, () => accumulator)(extractResult, extractorName)(timeExtractor)
}

case class Aligned[Event, InnerState, T](by: Window, phase: PhaseParser[Event, InnerState, T])
                                        (implicit timeExtractor: TimeExtractor[Event])
     extends PhaseParser[Event, (InnerState, Option[Time]), T] {
  require(by.toMillis > 0, "For performance reasons you cannot have empty alignment.")

  override def initialState = (phase.initialState, None)

  override def apply(event: Event, state: (InnerState, Option[Time])): (PhaseResult[T], (InnerState, Option[Time])) = {
    val currTime = timeExtractor(event)
    val (phaseState, startTimeOpt) = state
    startTimeOpt match {
      case Some(startTime) =>
        if (currTime >= startTime.plus(by)) {
          val (result, newState) = phase(event, phaseState)
          result -> (newState, startTimeOpt)
        }
        else Stay -> state
      case None =>
        Stay -> (phaseState, Some(currTime))
    }
  }

  override def format(event: Event, state: (InnerState, Option[Time])) = {
    val sec = by.toMillis / 1000.0
    s"Aligned(by $sec sec)(${phase.format(event, state._1)})"
  }
}


trait AccumulatedState[T] extends Serializable {

  def startTime: Option[Time]

  def lastTime: Option[Time]

  def updated(time: Time, value: T): AccumulatedState[T]

  def hasState: Boolean = startTime.isDefined

  def overallTimeMs: Option[Long] = for {
    start <- startTime
    last <- lastTime
  } yield last.toMillis - start.toMillis
}

// TODO T -> Unit
case class CountAccumulatedState[T](window: Window, count: Long = 0L, startTime: Option[Time] = None,
                                    lastTime: Option[Time] = None) extends AccumulatedState[T] {
  override def updated(time: Time, value: T) = {
    CountAccumulatedState(
      window = window,
      count = count + 1,
      startTime = startTime.orElse(Some(time)),
      lastTime = Some(time)
    )
  }
}

case class TruthAccumulatedState(window: Window, truthCount: Long = 0L, truthMillisCount: Long = 0L,
                                 prevValue: Boolean = false, startTime: Option[Time] = None,
                                 lastTime: Option[Time] = None) extends AccumulatedState[Boolean] {

  def updated(time: Time, value: Boolean): TruthAccumulatedState = {
    lastTime match {
      case Some(prevTime) =>
        // If first and prev value is true - add time between it and current value to
        // millis accumulator (or it won't be accounted at all)
        val msToAddForPrevValue = if (prevValue && truthMillisCount == 0L) time.toMillis - prevTime.toMillis else 0L
        val currentTruthMs = if (value) time.toMillis - prevTime.toMillis else 0L
        TruthAccumulatedState(
          window = window,
          truthCount = truthCount + (if (value) 1 else 0),
          truthMillisCount = truthMillisCount + currentTruthMs + msToAddForPrevValue,
          startTime = startTime.orElse(Some(time)),
          lastTime = Some(time),
          prevValue = value
        )
      case None =>
        TruthAccumulatedState(
          window = window,
          truthCount = truthCount + (if (value) 1 else 0),
          truthMillisCount = 0L,
          startTime = startTime.orElse(Some(time)),
          lastTime = Some(time),
          prevValue = value
        )
    }
  }
}



case class NumericAccumulatedState(window: Window, sum: Double = 0d, count: Long = 0l,
                                   startTime: Option[Time] = None, lastTime: Option[Time] = None)
  extends AccumulatedState[Double] {

  def updated(time: Time, value: Double): NumericAccumulatedState = {
    val a = 123
    NumericAccumulatedState(
      window = window,
      sum = sum + value,
      count = count + 1,
      startTime = startTime.orElse(Some(time)),
      lastTime = Some(time)
    )
  }

  def avg: Double = {
    assert(count != 0, "Illegal state: avg shouldn't be called on empty state.")
    sum / count
  }
}