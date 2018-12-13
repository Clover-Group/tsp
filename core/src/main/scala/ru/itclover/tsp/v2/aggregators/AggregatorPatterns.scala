package ru.itclover.tsp.v2.aggregators

import cats.{Functor, Monad}

import scala.Ordering.Implicits._
import ru.itclover.tsp.core.Time._
import ru.itclover.tsp.v2._
import ru.itclover.tsp.core.{Time, Window}
import ru.itclover.tsp.patterns.Booleans.BooleanPhaseParser
import ru.itclover.tsp.patterns.Combining.And
import ru.itclover.tsp._
import ru.itclover.tsp.aggregators.accums._
import ru.itclover.tsp.aggregators.accums.OneTimeStates._
import ru.itclover.tsp.core.PatternResult.Stay
import ru.itclover.tsp.io.TimeExtractor
import ru.itclover.tsp.patterns.Numerics.NumericPhaseParser
import ru.itclover.tsp.v2.Extract._
import ru.itclover.tsp.v2.aggregators.accums.{AccumPattern, AccumState, AggregatorPState, TimerAccumState}

import scala.language.higherKinds
import scala.collection.immutable.Queue

trait AggregatorPatterns[Event, T, S <: PState[T, S], F[_], Cont[_]] extends Pattern[Event, T, S, F, Cont]

object AggregatorPhases {

  trait AggregatorFunctions[F[_], Cont[_]] extends AccumFunctions {

    def lag[Event: IdxExtractor: TimeExtractor, Out, S <: PState[Out, S]](
      pattern: Pattern[Event, Out, S, F, Cont],
      window: Window
    )(implicit ev1: Monad[F], ev2: Functor[Cont], ev3: AddToQueue[Cont]) = PreviousValue(pattern, window)

//    def delta[Event, S](
//      numeric: NumericPhaseParser[Event, S]
//    )(implicit timeExtractor: TimeExtractor[Event]): NumericPhaseParser[Event, _] =
//      numeric minus PreviousValue(numeric)
//
//    def deltaMillis[Event, S, T](
//      phase: Pattern[Event, S, T]
//    )(implicit timeExtractor: TimeExtractor[Event]): NumericPhaseParser[Event, _] =
//      CurrentTimeMs(phase) minus PreviousTimeMs(phase)

  }

  //todo MinParser, MaxParser, MedianParser, ConcatParser, Timer

//  /**
//    * Can be useful in andThen phase (to apply next step to next event after success on left, not the same one)
//    */
//  case class Skip[Event, InnerState, T](numEvents: Int, phase: Pattern[Event, InnerState, T])
//      extends Pattern[Event, (InnerState, Option[Int]), T] {
//    require(numEvents > 0)
//
//    override def initialState: (InnerState, Option[Int]) = (phase.initialState, None)
//
//    override def apply(event: Event, state: (InnerState, Option[Int])) = {
//      val (innerState, skippedOpt) = state
//      skippedOpt match {
//        case Some(skipped) =>
//          if (skipped >= numEvents) {
//            val (innerResult, newInnerState) = phase(event, innerState)
//            innerResult -> (newInnerState, Some(skipped + 1))
//          } else {
//            Stay -> (innerState, Some(skipped + 1))
//          }
//
//        case None => Stay -> (innerState, Some(1))
//      }
//    }
//
//    override def format(event: Event, state: (InnerState, Option[Int])): String =
//      s"Skip($numEvents, ${phase.format(event, state._1)})"
//  }

  case class PreviousValue[Event: IdxExtractor: TimeExtractor, State <: PState[Out, State], Out, F[_]: Monad, Cont[_]: Functor: AddToQueue](
    override val innerPattern: Pattern[Event, Out, State, F, Cont],
    override val window: Window
  ) extends AccumPattern[Event, State, Out, Out, PreviousValueAccumState[Out], F, Cont] {
    override def initialState(): AggregatorPState[State, PreviousValueAccumState[Out], Out] =
      AggregatorPState(innerPattern.initialState(), PreviousValueAccumState(None), Queue.empty, Queue.empty)
  }
//      with AggregatorPatterns[Event, Out, State And Option[Out], F, Cont]

  case class PreviousValueAccumState[T](lastTimeValue: Option[(Time, T)])
      extends AccumState[T, T, PreviousValueAccumState[T]] {
    override def updated(window: Window, idx: Idx, time: Time, value: Result[T]): (PreviousValueAccumState[T], QI[T]) = {
      value match {
        case Fail => this -> Queue(IdxValue(idx, Result.fail))
        case Succ(newValue) =>
          PreviousValueAccumState(Some(time -> newValue)) ->
          this.lastTimeValue
            .filter(_._1.plus(window) >= time)
            .map(ab => Queue(IdxValue(idx, Result.succ(ab._2))))
            .getOrElse(Queue.empty)
      }
    }
  }

//  case class CurrentTimeMs[Event, State, T](innerPhase: Pattern[Event, State, T])(
//    implicit timeExtractor: TimeExtractor[Event]
//  ) extends NumericPhaseParser[Event, State] {
//
//    override def apply(event: Event, state: State) = {
//      val t = timeExtractor(event)
//      val (innerResult, newState) = innerPhase(event, state)
//      (innerResult match {
//        case Success(_) => Success(t.toMillis.toDouble)
//        case x @ Stay   => x
//        case Failure(x) => Failure(x)
//      }) -> newState
//    }
//
//    override def initialState = innerPhase.initialState
//
//    override def format(event: Event, state: State) =
//      s"prev(${innerPhase.format(event, state)})"
//  }
//
//  case class PreviousTimeMs[Event, State, T](innerPhase: Pattern[Event, State, T])(
//    implicit timeExtractor: TimeExtractor[Event]
//  ) extends NumericPhaseParser[Event, State And Option[Double]] {
//
//    override def apply(event: Event, state: (State, Option[Double])) = {
//      val t = timeExtractor(event)
//      val (innerState, prevMsOpt) = state
//      val (innerResult, newInnerState) = innerPhase(event, innerState)
//
//      (innerResult, prevMsOpt) match {
//        case (Success(v), Some(prevMs))      => Success(prevMs) -> (newInnerState -> Some(t.toMillis))
//        case (Success(v), None)              => Stay            -> (newInnerState -> Some(t.toMillis))
//        case (Stay, prevOpt: Option[Double]) => Stay            -> (newInnerState -> prevOpt)
//        case (f: Failure, _)                 => f               -> initialState
//      }
//    }
//
//    override def initialState = innerPhase.initialState -> None
//  }

//  /**
//    * Compute derivative by time.
//    *
//    * @param innerPhase inner numeric parser
//    */
//  case class Derivation[Event, InnerState](innerPhase: NumericPhaseParser[Event, InnerState])(
//    implicit extractTime: TimeExtractor[Event]
//  ) extends AggregatorPhases[Event, InnerState And Option[Double And Time], Double] {
//
//    override def apply(
//      event: Event,
//      state: InnerState And Option[Double And Time]
//    ): (PatternResult[Double], InnerState And Option[Double And Time]) = {
//      val t = extractTime(event)
//      val (innerState, prevValueAndTimeOpt) = state
//      val (innerResult, newInnerState) = innerPhase(event, innerState)
//
//      (innerResult, prevValueAndTimeOpt) match {
//        case (Success(v), Some((prevValue, prevTime))) =>
//          Success(deriv(prevValue, v, prevTime.toMillis, t.toMillis)) -> (newInnerState -> Some(v, t))
//        case (Success(v), None)      => Stay -> (newInnerState -> Some(v, t))
//        case (Stay, Some(prevValue)) => Stay -> (newInnerState, Some(prevValue)) // pushing prev value on stay phases?
//        case (Stay, None)            => Stay -> (newInnerState -> None)
//        case (f: Failure, _)         => f    -> initialState
//      }
//
//    }
//
//    def deriv(x1: Double, x2: Double, y1: Double, y2: Double) = (x2 - x1) / (y2 - y1)
//
//    override def initialState = (innerPhase.initialState, None)
//
//    override def format(event: Event, state: (InnerState, Option[(Double, Time)])) = {
//      val result = state._2.map("=" + _._1.toString).getOrElse("")
//      s"deriv(${innerPhase.format(event, state._1)})" + result
//    }
//  }
//
//  /**
//    * Accumulates Stay and consequent Success to a single Success [[Segment]]
//    *
//    * @param innerPhase   - parser to wrap with gaps
//    * @param timeExtractor - function returning time from Event
//    * @tparam Event - events to process
//    * @tparam State - type of state for innerParser
//    */
//  case class ToSegments[Event, State, Out](innerPhase: Pattern[Event, State, Out])(
//    implicit timeExtractor: TimeExtractor[Event]
//  ) extends AggregatorPhases[Event, State And Option[Time], Segment] {
//    // TODO Add max gap interval i.e. timeout, e.g. `maxGapInterval: TimeInterval`:
//    // e.g. state(inner, start, prev) -> if curr - prev > maxGapInterval (start, prev) else (start, curr)
//
//    override def apply(event: Event, state: State And Option[Time]): (PatternResult[Segment], State And Option[Time]) = {
//      val eventTime = timeExtractor(event)
//      val (innerState, prevEventTimeOpt) = state
//
//      innerPhase(event, innerState) match {
//        // Return accumulated Stay and resulting Success as single segment
//        case (Success(_), newInnerState) =>
//          Success(Segment(prevEventTimeOpt.getOrElse(eventTime), eventTime)) -> (newInnerState -> None)
//
//        // TODO accumulate until timeout
//        case (Stay, newInnerState) => Stay -> (newInnerState -> prevEventTimeOpt.orElse(Some(eventTime)))
//
//        case (failure: Failure, newInnerState) => failure -> (newInnerState -> None)
//      }
//    }
//
//    override def initialState: (State, Option[Time]) = (innerPhase.initialState, None)
//
//    override def format(event: Event, state: (State, Option[Time])) = s"${innerPhase.format(event, state._1)} asSegments"
//  }

}
