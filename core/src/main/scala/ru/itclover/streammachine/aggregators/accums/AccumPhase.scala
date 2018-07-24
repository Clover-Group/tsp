package ru.itclover.streammachine.aggregators.accums

import ru.itclover.streammachine.aggregators.AggregatorPhases
import ru.itclover.streammachine.aggregators.accums.ContinuousStates.ContinuousAccumState
import ru.itclover.streammachine.core.{PhaseParser, PhaseResult, Time, Window}
import ru.itclover.streammachine.core.PhaseResult.{Failure, Stay, Success}
import ru.itclover.streammachine.core.Time.TimeExtractor
import scala.Ordering.Implicits._


class AccumPhase[Event, InnerState, AccumOut, Out]
    (innerPhase: PhaseParser[Event, InnerState, AccumOut], window: Window, accumulator: => AccumState[AccumOut])
    (extractResult: AccumState[AccumOut] => Out, extractorName: String)
    (implicit timeExtractor: TimeExtractor[Event])
  extends AggregatorPhases[Event, (InnerState, AccumState[AccumOut]), Out] {

  override def apply(event: Event, oldState: (InnerState, AccumState[AccumOut])):
    (PhaseResult[Out], (InnerState, AccumState[AccumOut])) =
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
        Failure(msg) -> (newInnerState -> oldAccumState)
      case Stay =>
        Stay -> (newInnerState -> oldAccumState)
    }
  }

  override def initialState: (InnerState, AccumState[AccumOut]) = innerPhase.initialState -> accumulator

  override def format(event: Event, state: (InnerState, AccumState[AccumOut])) = if (state._2.hasState) {
    s"$extractorName(${innerPhase.format(event, state._1)})=${extractResult(state._2)}"
  } else {
    s"$extractorName(${innerPhase.format(event, state._1)})"
  }
}


trait AccumState[T] extends Product with Serializable {

  def updated(time: Time, value: T): AccumState[T]

  def startTime: Option[Time]

  def lastTime: Option[Time]

  def overallTimeMs: Option[Long] = for {
    start <- startTime
    last <- lastTime
  } yield last.toMillis - start.toMillis

  def hasState: Boolean = startTime.isDefined && lastTime.isDefined
}
