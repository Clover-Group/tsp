package ru.itclover.tsp.aggregators

import scala.Ordering.Implicits._
import ru.itclover.tsp.core.PatternResult.{Failure, Stay, Success}
import ru.itclover.tsp.core.Time._
import ru.itclover.tsp.core.{Pattern, PatternResult, Time, Window}
import ru.itclover.tsp.patterns.Numerics.NumericPhaseParser
import ru.itclover.tsp._
import ru.itclover.tsp.aggregators.accums._
import ru.itclover.tsp.io.TimeExtractor
import ru.itclover.tsp.utils.UtilityTypes.And


trait AggregatorPhases[Event, S, T] extends Pattern[Event, S, T]

object AggregatorPhases {


  trait AggregatorFunctions extends AccumFunctions {

    def lag[Event, S, Out](phase: Pattern[Event, S, Out]) = PreviousValue(phase)

    def delta[Event, S](numeric: NumericPhaseParser[Event, S])
                       (implicit timeExtractor: TimeExtractor[Event]): NumericPhaseParser[Event, _] =
      numeric minus PreviousValue(numeric)

    def deltaMillis[Event, S, T](phase: Pattern[Event, S, T])
                                (implicit timeExtractor: TimeExtractor[Event]): NumericPhaseParser[Event, _] =
      CurrentTimeMs(phase) minus PreviousTimeMs(phase)


  }

  //todo MinParser, MaxParser, MedianParser, ConcatParser, Timer


  /**
    * Can be useful in andThen phase (to apply next step to next event after success on left, not the same one)
    */
  case class Skip[Event, InnerState, T](numEvents: Int, phase: Pattern[Event, InnerState, T])
       extends Pattern[Event, (InnerState, Option[Int]), T] {
    require(numEvents > 0)

    override def initialState: (InnerState, Option[Int]) = (phase.initialState, None)

    override def apply(event: Event, state: (InnerState, Option[Int])) = {
      val (innerState, skippedOpt) = state
      skippedOpt match {
        case Some(skipped) =>
          if (skipped >= numEvents) {
            val (innerResult, newInnerState) = phase(event, innerState)
            innerResult -> (newInnerState, Some(skipped + 1))
          } else {
            Stay -> (innerState, Some(skipped + 1))
          }

        case None => Stay -> (innerState, Some(1))
      }
    }

    override def format(event: Event, state: (InnerState, Option[Int])): String =
      s"Skip($numEvents, ${phase.format(event, state._1)})"
  }

  /**
    * Phase, mainly for technical purposes - to align OneTime accumulators in simple cases,
    * for example `avg(‘speed, 50 min) > avg(‘speed, 5 min)` compares 0-50 min and 0-5 min values, but:
    * `avg(‘speed, 50 min) > Aligned(45 min, avg(‘speed, 5 min))` compare 0-50 min and 45-50, as expected
    */
  case class Aligned[Event, InnerState, T](by: Window, phase: Pattern[Event, InnerState, T])
                                          (implicit timeExtractor: TimeExtractor[Event])
       extends Pattern[Event, (InnerState, Option[Time]), T] {
    require(by.toMillis > 0, "For performance reasons you cannot have empty alignment.")

    override def initialState = (phase.initialState, None)

    override def apply(event: Event, state: (InnerState, Option[Time])): (PatternResult[T], (InnerState, Option[Time])) = {
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


  case class PreviousValue[Event, State, Out](innerPhase: Pattern[Event, State, Out])
    extends AggregatorPhases[Event, State And Option[Out], Out] {

    override def apply(event: Event, state: (State, Option[Out])) = {
      val (innerState, prevValueOpt) = state
      val (innerResult, newInnerState) = innerPhase(event, innerState)

      (innerResult, prevValueOpt) match {
        case (Success(v), Some(prev)) => Success(prev) -> (newInnerState -> Some(v))
        case (Success(v), None) => Stay -> (newInnerState -> Some(v))
        case (Stay, prevOpt: Option[Out]) => Stay -> (newInnerState -> prevOpt)
        case (f: Failure, _) => f -> initialState
      }
    }

    override def initialState = innerPhase.initialState -> None

    override def format(event: Event, state: (State, Option[Out])) =
      s"prev(${innerPhase.format(event, state._1)})" + state._2.map(v => s"=$v").getOrElse("")

  }

  case class CurrentTimeMs[Event, State, T](innerPhase: Pattern[Event, State, T])
                                           (implicit timeExtractor: TimeExtractor[Event])
    extends NumericPhaseParser[Event, State] {


    override def apply(event: Event, state: State) = {
      val t = timeExtractor(event)
      val (innerResult, newState) = innerPhase(event, state)
      (innerResult match {
        case Success(_) => Success(t.toMillis.toDouble)
        case x@Stay => x
        case Failure(x) => Failure(x)
      }) -> newState
    }

    override def initialState = innerPhase.initialState

    override def format(event: Event, state: State) =
      s"prev(${innerPhase.format(event, state)})"
  }

  case class PreviousTimeMs[Event, State, T](innerPhase: Pattern[Event, State, T])
                                            (implicit timeExtractor: TimeExtractor[Event])
    extends NumericPhaseParser[Event, State And Option[Double]] {

    override def apply(event: Event, state: (State, Option[Double])) = {
      val t = timeExtractor(event)
      val (innerState, prevMsOpt) = state
      val (innerResult, newInnerState) = innerPhase(event, innerState)

      (innerResult, prevMsOpt) match {
        case (Success(v), Some(prevMs)) => Success(prevMs) -> (newInnerState -> Some(t.toMillis))
        case (Success(v), None) => Stay -> (newInnerState -> Some(t.toMillis))
        case (Stay, prevOpt: Option[Double]) => Stay -> (newInnerState -> prevOpt)
        case (f: Failure, _) => f -> initialState
      }
    }

    override def initialState = innerPhase.initialState -> None
  }


  /**
    * Compute derivative by time.
    * @param innerPhase inner numeric parser
    */
  case class Derivation[Event, InnerState](innerPhase: NumericPhaseParser[Event, InnerState])
                                          (implicit extractTime: TimeExtractor[Event])
    extends
      AggregatorPhases[Event, InnerState And Option[Double And Time], Double] {

    override def apply(event: Event, state: InnerState And Option[Double And Time]):
    (PatternResult[Double], InnerState And Option[Double And Time]) = {
      val t = extractTime(event)
      val (innerState, prevValueAndTimeOpt) = state
      val (innerResult, newInnerState) = innerPhase(event, innerState)

      (innerResult, prevValueAndTimeOpt) match {
        case (Success(v), Some((prevValue, prevTime))) =>
          Success(deriv(prevValue, v, prevTime.toMillis, t.toMillis)) -> (newInnerState -> Some(v, t))
        case (Success(v), None) => Stay -> (newInnerState -> Some(v, t))
        case (Stay, Some(prevValue)) => Stay -> (newInnerState, Some(prevValue)) // pushing prev value on stay phases?
        case (Stay, None) => Stay -> (newInnerState -> None)
        case (f: Failure, _) => f -> initialState
      }

    }

    def deriv(x1: Double, x2: Double, y1: Double, y2: Double) = (x2 - x1) / (y2 - y1)

    override def initialState = (innerPhase.initialState, None)

    override def format(event: Event, state: (InnerState, Option[(Double, Time)])) = {
      val result = state._2.map("=" + _._1.toString).getOrElse("")
      s"deriv(${innerPhase.format(event, state._1)})" + result
    }
  }


  /**
    * Accumulates Stay and consequent Success to a single Success [[Segment]]
    *
    * @param inner   - parser to wrap with gaps
    * @param timeExtractor - function returning time from Event
    * @tparam Event - events to process
    * @tparam State - type of state for innerParser
    */
  case class SegmentsPattern[Event, State, Out](inner: Pattern[Event, State, Out])
                                               (implicit timeExtractor: TimeExtractor[Event])
    extends AggregatorPhases[Event, State And Option[Time], Segment] {
    // TODO Add max gap interval i.e. timeout, e.g. `maxGapInterval: TimeInterval`:
    // e.g. state(inner, start, prev) -> if curr - prev > maxGapInterval (start, prev) else (start, curr)

    override def apply(event: Event, state: State And Option[Time]): (PatternResult[Segment], (State, Option[Time])) = {
      val eventTime = timeExtractor(event)
      val (innerState, prevEventTimeOpt) = state

      inner(event, innerState) match {
        // Return accumulated Stay and resulting Success as single segment
        case (Success(_), newInnerState) =>
          Success(Segment(prevEventTimeOpt.getOrElse(eventTime), eventTime)) -> (newInnerState -> None)

        // TODO accumulate until timeout
        case (Stay, newInnerState) => Stay -> (newInnerState -> prevEventTimeOpt.orElse(Some(eventTime)))

        case (failure: Failure, newInnerState) => failure -> (newInnerState -> None)
      }
    }

    override def initialState: (State, Option[Time]) = (inner.initialState, None)

    override def format(event: Event, state: (State, Option[Time])) = s"${inner.format(event, state._1)} asSegments"
  }

}
