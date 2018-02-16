package ru.itclover.streammachine.aggregators

import ru.itclover.streammachine.core.PhaseResult.{Failure, Stay, Success}
import ru.itclover.streammachine.core.Time._
import ru.itclover.streammachine.core.{PhaseParser, PhaseResult, Time, Window}
import ru.itclover.streammachine.phases.CombiningPhases.And
import ru.itclover.streammachine.phases.NumericPhases.NumericPhaseParser

import scala.Ordering.Implicits._
import scala.annotation.tailrec
import scala.collection.immutable.Queue
import scala.math.Numeric.Implicits._

trait AggregatorPhases[Event, S, T] extends PhaseParser[Event, S, T]

object AggregatorPhases {

  trait AggregatorFunctions {

    def avg[Event, S](numeric: NumericPhaseParser[Event, S], window: Window)
                     (implicit timeExtractor: TimeExtractor[Event]): Average[Event, S] =
      Average(numeric, window)


    def derivation[Event, S](numeric: NumericPhaseParser[Event, S])
                            (implicit timeExtractor: TimeExtractor[Event]): Derivation[Event, S] =
      Derivation(numeric)
  }

  case class Segment(from: Time, to: Time)

  type ValueAndTime = Double And Time

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
    extends AggregatorPhases[Event, (InnerState, AverageState[Double]), Double] {

    override def apply(event: Event, oldState: (InnerState, AverageState[Double])): (PhaseResult[Double], (InnerState, AverageState[Double])) = {
      val time = timeExtractor(event)
      val (oldInnerState, oldAverageState) = oldState
      val (innerResult, newInnerState) = extract(event, oldInnerState)

      innerResult match {
        case Success(t) => {
          val newAverageState = oldAverageState.updated(time, t)

          val newAverageResult = newAverageState.startTime match {
            case Some(startTime) if time >= startTime.plus(window) => Success(newAverageState.result)
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
    * Accumulates Stay and consequent Success to a single Success [[Segment]]
    *
    * @param innerParser   - parser to wrap with gaps
    * @param timeExtractor - function returning time from Event
    * @tparam Event - events to process
    * @tparam State - type of state for innerParser
    */
  case class ToSegments[Event, State, Out](innerParser: PhaseParser[Event, State, Out])
                                          (implicit timeExtractor: TimeExtractor[Event])
    extends AggregatorPhases[Event, State And Option[Time], Segment] {
    // TODO Add max gap interval i.e. timeout, e.g. `maxGapInterval: TimeInterval`:
    // e.g. state(inner, start, prev) -> if curr - prev > maxGapInterval (start, prev) else (start, curr)

    override def apply(event: Event, state: State And Option[Time]):
    (PhaseResult[Segment], State And Option[Time]) = {
      val eventTime = timeExtractor(event)
      val (innerState, prevEventTimeOpt) = state

      innerParser(event, innerState) match {
        // Return accumulated Stay and resulting Success as single segment
        case (Success(_), newInnerState) =>
          Success(Segment(prevEventTimeOpt.getOrElse(eventTime), eventTime)) -> (newInnerState -> None)

        // TODO accumulate until timeout
        case (Stay, newInnerState) => Stay -> (newInnerState -> prevEventTimeOpt.orElse(Some(eventTime)))

        case (failure: Failure, newInnerState) => failure -> (newInnerState -> None)
      }
    }

    override def initialState: (State, Option[Time]) = (innerParser.initialState, None)
  }


  /**
    * Computation of derivative by time (todo).
    *
    * @param numeric inner numeric parser
    */
  case class Derivation[Event, InnerState](numeric: NumericPhaseParser[Event, InnerState])
                                          (implicit extractTime: TimeExtractor[Event])
    extends
      AggregatorPhases[Event, InnerState And Option[ValueAndTime], Double] {

    // TODO: Add time
    override def apply(event: Event, state: InnerState And Option[ValueAndTime]):
    (PhaseResult[Double], InnerState And Option[ValueAndTime]) = {
      val t = extractTime(event)
      val (innerState, prevValueAndTimeOpt) = state
      val (innerResult, newInnerState) = numeric(event, innerState)

      (innerResult, prevValueAndTimeOpt) match {
        case (Success(v), Some((prevValue, prevTime))) =>
          Success(v - prevValue) -> (newInnerState, Some(v, t))
        case (Success(v), None) => Stay -> (newInnerState, Some(v, t))
        case (Stay, Some(prevValue)) => Stay -> (newInnerState, Some(prevValue)) // pushing prev value on stay phases
        case (Stay, None) => Stay -> (newInnerState, None)
        case (f: Failure, _) => f -> initialState
      }

    }

    override def initialState = (numeric.initialState, None)
  }

}
