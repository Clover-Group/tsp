package ru.itclover.tsp.aggregators.accums

import ru.itclover.tsp.aggregators.AggregatorPhases
import ru.itclover.tsp.core.PatternResult.{Failure, Stay, Success}
import ru.itclover.tsp.core._
import ru.itclover.tsp.core.Intervals._

case class PushDownAccumInterval[Event, InnerState, AccumOut, Out](
  accum: AccumPhase[Event, InnerState, AccumOut, Out],
  interval: Interval[Out]
) extends AggregatorPhases[Event, (InnerState, AccumState[AccumOut]), Out] {

  override def initialState = accum.initialState

  override def apply(event: Event, state: (InnerState, AccumState[AccumOut])) = {
    val (newResult, (newInnerState, newAccumState)) = accum.apply(event, state)
    val accumOut = accum.extractResult(newAccumState)
    val pushedResult = newResult match {
      case Stay =>
        interval.getRelativePosition(accumOut) match {
          case GreaterThanEnd                => Failure(s"Accum value `$accumOut` went outside of interval `$interval`.")
          case Inside if interval.isInfinite => Success(accumOut)

          case Inside | LessThanBegin => newResult
        }
      case _ => newResult // result, other than Stay - return as it is
    }

    (pushedResult, (newInnerState, newAccumState))
  }
}
