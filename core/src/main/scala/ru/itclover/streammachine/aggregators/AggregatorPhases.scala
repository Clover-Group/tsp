package ru.itclover.streammachine.aggregators

import ru.itclover.streammachine.core.PhaseResult.{Failure, Stay, Success}
import ru.itclover.streammachine.core.Time._
import ru.itclover.streammachine.core.{PhaseParser, PhaseResult, Time, Window}
import ru.itclover.streammachine.phases.CombiningPhases.{And, TogetherParserLike}
import ru.itclover.streammachine.phases.NumericPhases.NumericPhaseParser
import ru.itclover.streammachine.phases.TimePhases.Wait

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

    def sum[Event, S](numeric: NumericPhaseParser[Event, S], window: Window)
                     (implicit timeExtractor: TimeExtractor[Event]): Sum[Event, S] =
      Sum(numeric, window)

    def count[Event, S](numeric: NumericPhaseParser[Event, S], window: Window)
                     (implicit timeExtractor: TimeExtractor[Event]): Count[Event, S] =
      Count(numeric, window)

    def derivation[Event, S](numeric: NumericPhaseParser[Event, S])
                            (implicit timeExtractor: TimeExtractor[Event]): NumericPhaseParser[Event, _] = {
      delta(numeric) / deltaMilliseconds(numeric).map(dt => {
        assert(dt != 0.0, "Zero division - delta time in derivation is 0.")
        dt
      })
    }

    def delta[Event, S](numeric: NumericPhaseParser[Event, S])
                       (implicit timeExtractor: TimeExtractor[Event]): NumericPhaseParser[Event, _] =
      numeric - PreviousValue(numeric)

    def deltaMilliseconds[Event, S, T](phase: PhaseParser[Event, S, T])
                                      (implicit timeExtractor: TimeExtractor[Event]): NumericPhaseParser[Event, _] =
      PreviousTimeMs(phase) - CurrentTimeMs(phase)


  }

  case class Segment(from: Time, to: Time)

  type ValueAndTime = Double And Time

  case class AccumulatedState(window: Window, sum: Double = 0d, count: Long = 0l, queue: Queue[(Time, Double)] = Queue.empty) extends Serializable {

    def updated(time: Time, value: Double): AccumulatedState = {

      @tailrec
      def removeOldElementsFromQueue(as: AccumulatedState): AccumulatedState =
        as.queue.dequeueOption match {
          case Some(((oldTime, oldValue), newQueue)) if oldTime.plus(window) < time =>
            removeOldElementsFromQueue(
              AccumulatedState(
                window = window,
                sum = sum - oldValue.toDouble(),
                count = count - 1,
                queue = newQueue
              )
            )
          case _ => as
        }

      val cleaned = removeOldElementsFromQueue(this)

      AccumulatedState(window,
        sum = cleaned.sum + value.toDouble(),
        count = cleaned.count + 1,
        queue = cleaned.queue.enqueue((time, value)))
    }

    def avg: Double = {
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
  abstract class AccumulationPhase[Event, InnerState](extract: NumericPhaseParser[Event, InnerState], window: Window)
                                                     (extractResult: AccumulatedState => Double)
                                                     (implicit timeExtractor: TimeExtractor[Event])
    extends AggregatorPhases[Event, (InnerState, AccumulatedState), Double] {

    override def apply(event: Event, oldState: (InnerState, AccumulatedState)): (PhaseResult[Double], (InnerState, AccumulatedState)) = {
      val time = timeExtractor(event)
      val (oldInnerState, oldAverageState) = oldState
      val (innerResult, newInnerState) = extract(event, oldInnerState)

      innerResult match {
        case Success(t) => {
          val newAverageState = oldAverageState.updated(time, t)

          val newAverageResult = newAverageState.startTime match {
            case Some(startTime) if time >= startTime.plus(window) => Success(extractResult(newAverageState))
            case _ => Stay
          }

          newAverageResult -> (newInnerState -> newAverageState)
        }
        case f@Failure(msg) =>
          Failure(msg) -> (newInnerState -> oldAverageState)
        case Stay => Stay -> (newInnerState -> oldAverageState)
      }
    }

    override def initialState: (InnerState, AccumulatedState) = extract.initialState -> AccumulatedState(window)
  }

  case class Average[Event, InnerState](innerPhase: NumericPhaseParser[Event, InnerState], window: Window)
                                       (implicit timeExtractor: TimeExtractor[Event])
    extends AccumulationPhase[Event, InnerState](innerPhase, window)(_.avg) {

    override def format(event: Event, state: (InnerState, AccumulatedState)) = if (state._2.count == 0) {
      s"avg(${innerPhase.format(event, state._1)})"
    } else {
      s"avg(${innerPhase.format(event, state._1)})=${state._2.sum}/${state._2.count}=${state._2.avg}"
    }
  }

  case class Sum[Event, InnerState](innerPhase: NumericPhaseParser[Event, InnerState], window: Window)
                                   (implicit timeExtractor: TimeExtractor[Event])
    extends AccumulationPhase[Event, InnerState](innerPhase, window)(_.sum) {

    override def format(event: Event, state: (InnerState, AccumulatedState)) = if (state._2.count == 0) {
      s"sum(${innerPhase.format(event, state._1)})"
    } else {
      s"sum(${innerPhase.format(event, state._1)})=${state._2.sum}"
    }
  }

  case class Count[Event, InnerState](innerPhase: NumericPhaseParser[Event, InnerState], window: Window)
                                     (implicit timeExtractor: TimeExtractor[Event])
    extends AccumulationPhase[Event, InnerState](innerPhase, window)(_.count) {

    override def format(event: Event, state: (InnerState, AccumulatedState)) = if (state._2.count == 0) {
      s"count(${innerPhase.format(event, state._1)})"
    } else {
      s"count(${innerPhase.format(event, state._1)})=${state._2.count}"
    }
  }


  //todo MinParser, MaxParser, CountParser, MedianParser, ConcatParser, Timer

  case class PreviousValue[Event, State](innerPhase: NumericPhaseParser[Event, State])
                                        (implicit timeExtractor: TimeExtractor[Event])
    extends AggregatorPhases[Event, State And Option[Double], Double] {

    override def apply(event: Event, state: (State, Option[Double])) = {
      val t = timeExtractor(event)
      val (innerState, prevValueOpt) = state
      val (innerResult, newInnerState) = innerPhase(event, innerState)

      (innerResult, prevValueOpt) match {
        case (Success(v), Some(prev)) => Success(prev) -> (newInnerState -> Some(v))
        case (Success(v), None) => Stay -> (newInnerState -> Some(v))
        case (Stay, prevOpt: Option[Double]) => Stay -> (newInnerState -> prevOpt)
        case (f: Failure, _) => f -> initialState
      }
    }

    override def initialState = innerPhase.initialState -> None

    override def format(event: Event, state: (State, Option[Double])) =
      s"prev(${innerPhase.format(event, state._1)})" + state._2.map(v => s"=$v").getOrElse("")

  }

  case class CurrentTimeMs[Event, State, T](innerPhase: PhaseParser[Event, State, T])
                                           (implicit timeExtractor: TimeExtractor[Event])
    extends NumericPhaseParser[Event, State] {


    override def apply(event: Event, state: State) = {
      val t = timeExtractor(event)
      val (innerResult, newState) = innerPhase(event, state)
      (innerResult match {
        case Success(_) => Success(t.toMillis.toDouble)
        case Stay => Stay
        case x: Failure => x
      }) -> newState
    }

    override def initialState = innerPhase.initialState

    override def format(event: Event, state: State) =
      s"prev(${innerPhase.format(event, state)})"
  }

  case class PreviousTimeMs[Event, State, T](innerPhase: PhaseParser[Event, State, T])
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
      AggregatorPhases[Event, InnerState And Option[ValueAndTime], Double] {

    override def apply(event: Event, state: InnerState And Option[ValueAndTime]):
    (PhaseResult[Double], InnerState And Option[ValueAndTime]) = {
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
    * @param innerPhase   - parser to wrap with gaps
    * @param timeExtractor - function returning time from Event
    * @tparam Event - events to process
    * @tparam State - type of state for innerParser
    */
  case class ToSegments[Event, State, Out](innerPhase: PhaseParser[Event, State, Out])
                                          (implicit timeExtractor: TimeExtractor[Event])
    extends AggregatorPhases[Event, State And Option[Time], Segment] {
    // TODO Add max gap interval i.e. timeout, e.g. `maxGapInterval: TimeInterval`:
    // e.g. state(inner, start, prev) -> if curr - prev > maxGapInterval (start, prev) else (start, curr)

    override def apply(event: Event, state: State And Option[Time]):
    (PhaseResult[Segment], State And Option[Time]) = {
      val eventTime = timeExtractor(event)
      val (innerState, prevEventTimeOpt) = state

      innerPhase(event, innerState) match {
        // Return accumulated Stay and resulting Success as single segment
        case (Success(_), newInnerState) =>
          Success(Segment(prevEventTimeOpt.getOrElse(eventTime), eventTime)) -> (newInnerState -> None)

        // TODO accumulate until timeout
        case (Stay, newInnerState) => Stay -> (newInnerState -> prevEventTimeOpt.orElse(Some(eventTime)))

        case (failure: Failure, newInnerState) => failure -> (newInnerState -> None)
      }
    }

    override def initialState: (State, Option[Time]) = (innerPhase.initialState, None)

    override def format(event: Event, state: (State, Option[Time])) = s"${innerPhase.format(event, state._1)} asSegments"
  }

}
