package ru.itclover.streammachine.core

import java.time.Instant

import ru.itclover.streammachine.core.Aggregators.Average
import ru.itclover.streammachine.core.PhaseParser.And
import ru.itclover.streammachine.core.PhaseResult.{Failure, Stay, Success}
import ru.itclover.streammachine.core.Time._

import scala.Ordering.Implicits._
import scala.annotation.tailrec
import scala.collection.immutable.Queue
import scala.math.Numeric.Implicits._

trait AggregatingPhaseParser[Event, S] extends NumericPhaseParser[Event, S]

object AggregatingPhaseParser {

  def avg[Event, S](numeric: NumericPhaseParser[Event, S], window: Window)(implicit timeExtractor: TimeExtractor[Event]): Average[Event, S] = Average(numeric, window)


  def derivation[Event, S](numeric: NumericPhaseParser[Event, S]): Derivation[Event, S] = Derivation(numeric)
}

object Aggregators {

  case class AverageState[T: Numeric]
  (window: Window, sum: Double = 0d, count: Long = 0l, queue: Queue[(Time, T)] = Queue.empty) {

    def updated(time: Time, value: T): AverageState[T] = {

      @tailrec
      def removeOldElementsFromQueue(as: AverageState[T]): AverageState[T] =
        as.queue.dequeueOption match {
          case Some(((oldTime, oldValue), newQueue)) if oldTime.plus(window) < time =>
            removeOldElementsFromQueue(
              AverageState(
                window = window,
                sum = sum - oldValue.toDouble(),
                count = count - 1,
                queue = newQueue
              )
            )
          case _ => as
        }

      val cleaned = removeOldElementsFromQueue(this)

      AverageState(window,
        sum = cleaned.sum + value.toDouble(),
        count = cleaned.count + 1,
        queue = cleaned.queue.enqueue((time, value)))
    }

    def result: Double = {
      assert(count != 0, "Illegal state!") // it should n't be called on empty state
      sum / count
    }

    def startTime: Option[Time] = queue.headOption.map(_._1)
  }

  /**
    * PhaseParser collecting average value within window
    *
    * @param extract - function to extract value of collecting field from event
    * @param window  - window to collect points within
    * @tparam Event - events to process
    */
  case class Average[Event, InnerState](extract: NumericPhaseParser[Event, InnerState], window: Window)(implicit timeExtractor: TimeExtractor[Event])
    extends NumericPhaseParser[Event, (InnerState, AverageState[Double])] {

    override def apply(event: Event, oldState: (InnerState, AverageState[Double])): (PhaseResult[Double], (InnerState, AverageState[Double])) = {
      val time = timeExtractor(event)
      val (oldInnerState, oldAverageState) = oldState
      val (innerResult, newInnerState) = extract(event, oldInnerState)

      innerResult match {
        case Success(t, ctx) => {
          val newAverageState = oldAverageState.updated(time, t)

          val newAverageResult = newAverageState.startTime match {
            case Some(startTime) if time >= startTime.plus(window) => Success(newAverageState.result, ctx)
            case _ => Stay
          }

          newAverageResult -> (newInnerState -> newAverageState)
        }
        case f@Failure(msg) => Failure(msg) -> (newInnerState -> oldAverageState)
        case Stay => Stay -> (newInnerState -> oldAverageState)
      }
    }

    override def initialState: (InnerState, AverageState[Double]) = extract.initialState -> AverageState(window)
  }


  //todo MinParser, MaxParser, CountParser, MedianParser, ConcatParser, Timer

  /**
    * Timer parser. Returns:
    * Stay - if passed less than min boundary of timeInterval
    * Success - if passed time is between time interval
    * Failure - if passed more than max boundary of timeInterval
    *
    * @param timeInterval - time limits
    * @param timeExtractor - function returning time from Event
    * @tparam Event - events to process
    */
  case class Timer[Event](timeInterval: TimeInterval)
                         (implicit timeExtractor: TimeExtractor[Event])
    extends PhaseParser[Event, Option[Time], (Time, Time)] {

    override def apply(event: Event, state: Option[Time]): (PhaseResult[(Time, Time)], Option[Time]) = {

      val eventTime = timeExtractor(event)

      state match {
        case None =>
          Stay -> Some(eventTime)
        case Some(startTime) =>
          val result = if (startTime.plus(timeInterval.min) < eventTime) Stay
          else if (startTime.plus(timeInterval.max) <= eventTime) Success(startTime -> eventTime, Map.empty)
          else Failure(s"Timeout expired at $eventTime")

          result -> state
      }
    }

    override def initialState: Option[Time] = None
  }


  case class Segment(from: Time, to: Time)

  /**
    * Transform chain of Success results to one Success result in format of `(fromTime, toTime)`
    * @param innerParser - parser to wrap with gaps
    * @param timeExtractor - function returning time from Event
    * @tparam Event - events to process
    * @tparam State type of state for innerParser
    */
  case class ToSegments[Event, State, Out](innerParser: PhaseParser[Event, State, Out])
                                          (implicit timeExtractor: TimeExtractor[Event])
    extends PhaseParser[Event, State And Option[Time], Segment] {
    // TODO Add max gap interval i.e. timeout, e.g. `maxGapInterval: TimeInterval`:
    // e.g. state(inner, start, prev) -> if curr - prev > maxGapInterval (start, prev) else (start, curr)

    override def apply(event: Event, state: State And Option[Time]):
    (PhaseResult[Segment], State And Option[Time]) = {
      val eventTime = timeExtractor(event)
      val (innerState, prevEventTimeOpt) = state

      innerParser(event, innerState) match { // ?? Why computations stops if return Stay here? Bcs of одноразовость?
        // Return segment. If we `prevEventTimeOpt` is None then segment collapses to the one point.
        case (Success(_, _), newInnerState) => Success(
            Segment(prevEventTimeOpt.getOrElse(eventTime), eventTime), Map.empty
          ) -> (newInnerState -> None)
        // Accumulate here Stay as segment, if it not goes after success (i.e. segments not closing)
        // otherwise return the segment with previous Success value.
        case (Stay, newInnerState) => Stay -> (newInnerState -> prevEventTimeOpt.orElse(Some(eventTime)))

        case (failure: Failure, newInnerState) => failure  -> (newInnerState -> None)
      }
    }

    override def initialState: (State, Option[Time]) = (innerParser.initialState, None)
  }

}


case class Derivation[Event, InnerState](numeric: NumericPhaseParser[Event, InnerState]) extends
  NumericPhaseParser[Event, InnerState And Option[Double]] {

  // TODO: Add time
  override def apply(event: Event, state: InnerState And Option[Double]): (PhaseResult[Double], InnerState And Option[Double]) = {
    val (innerState, prevValueOpt) = state
    val (innerResult, newInnerState) = numeric(event, innerState)

    (innerResult, prevValueOpt) match {
      case (Success(t, ctx), Some(prevValue)) => Success(t - prevValue, ctx) -> (newInnerState, Some(t))
      case (Success(t, _), None) => Stay -> (newInnerState, Some(t))
      case (Stay, Some(prevValue)) => Stay -> (newInnerState, Some(prevValue)) // pushing prev value on stay phases
      // case (Stay, None) => Stay -> (newInnerState, None) // pushing prev value on stay phases // TODO
      case (f: Failure, _) => f -> initialState
    }

  }

  override def initialState = (numeric.initialState, None)
}
