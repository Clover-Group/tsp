package ru.itclover.streammachine.aggregators

import ru.itclover.streammachine.core.PhaseResult.{Failure, Stay, Success}
import ru.itclover.streammachine.core.Time._
import ru.itclover.streammachine.core.{PhaseParser, PhaseResult, Time, Window}
import ru.itclover.streammachine.phases.BooleanPhases.BooleanPhaseParser
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

    def avg[Event, S](numeric: PhaseParser[Event, S, Double], window: Window)
                     (implicit timeExtractor: TimeExtractor[Event]): AccumulationPhase[Event, S, Double, Double] = {
      AccumulationPhase(numeric, window, NumericAccumulatedState(window))({
        case a: NumericAccumulatedState => a.avg }, "avg")
    }

    def sum[Event, S](numeric: NumericPhaseParser[Event, S], window: Window)
                     (implicit timeExtractor: TimeExtractor[Event]): AccumulationPhase[Event, S, Double, Double] =
      AccumulationPhase(numeric, window, NumericAccumulatedState(window))({ case a: NumericAccumulatedState => a.sum }, "sum")

    def count[Event, S](numeric: NumericPhaseParser[Event, S], window: Window)
                       (implicit timeExtractor: TimeExtractor[Event]): AccumulationPhase[Event, S, Double, Long] =
      AccumulationPhase(numeric, window, NumericAccumulatedState(window))({ case a: NumericAccumulatedState => a.count }, "count")

    def millisCount[Event, S](numeric: NumericPhaseParser[Event, S], window: Window)
                             (implicit timeExtractor: TimeExtractor[Event]): AccumulationPhase[Event, S, Double, Long] =
      AccumulationPhase(numeric, window, NumericAccumulatedState(window))({ case a: NumericAccumulatedState => a.overallTimeMs }, "millisCount")


    def truthCount[Event, S](boolean: BooleanPhaseParser[Event, S], window: Window)
                            (implicit timeExtractor: TimeExtractor[Event]): AccumulationPhase[Event, S, Boolean, Long] = {
      AccumulationPhase(boolean, window, BoolAccumulatedState(window))({ case a: BoolAccumulatedState => a.truthCount }, "truthCount")
    }

    def truthMillisCount[Event, S](boolean: BooleanPhaseParser[Event, S], window: Window)
                                  (implicit timeExtractor: TimeExtractor[Event]): AccumulationPhase[Event, S, Boolean, Long] = {
      AccumulationPhase(boolean, window, BoolAccumulatedState(window))({ case a: BoolAccumulatedState => a.truthMillisCount }, "truthMillisCount")
    }


    def lag[Event, S, Out](phase: PhaseParser[Event, S, Out]) = PreviousValue(phase)


    def delta[Event, S](numeric: NumericPhaseParser[Event, S])
                       (implicit timeExtractor: TimeExtractor[Event]): NumericPhaseParser[Event, _] =
      numeric - PreviousValue(numeric)

    def deltaMillis[Event, S, T](phase: PhaseParser[Event, S, T])
                                (implicit timeExtractor: TimeExtractor[Event]): NumericPhaseParser[Event, _] =
      CurrentTimeMs(phase) - PreviousTimeMs(phase)


  }

  case class Segment(from: Time, to: Time)

  type ValueAndTime = Double And Time


  trait AccumulatedStateLike[T] {
    def startTime: Option[Time]

    def updated(time: Time, value: T): AccumulatedStateLike[T]

    def hasState: Boolean = startTime.isDefined
  }

  case class BoolAccumulatedState(window: Window, truthCount: Long = 0L, queue: Queue[(Time, Boolean)] = Queue.empty)
       extends Serializable with AccumulatedStateLike[Boolean] {

    def updated(time: Time, value: Boolean): BoolAccumulatedState = {
      @tailrec
      def removeOldElementsFromQueue(abs: BoolAccumulatedState): BoolAccumulatedState =
        abs.queue.dequeueOption match {
          case Some(((oldTime, oldVal), newQueue)) if oldTime.plus(window) < time =>
            removeOldElementsFromQueue(
              BoolAccumulatedState(
                window = window,
                truthCount = if (oldVal) truthCount - 1 else truthCount,
                queue = newQueue
              )
            )
          case _ => abs
        }

      val cleaned = removeOldElementsFromQueue(this)

      BoolAccumulatedState(window,
        truthCount = if (value) cleaned.truthCount + 1 else cleaned.truthCount,
        queue = cleaned.queue.enqueue((time, value))
      )
    }

    def startTime: Option[Time] = queue.headOption.map(_._1)

    def truthMillisCount = queue match {
      // if queue contains 2 and more elements
      case Queue((aTime, aVal), (bTime, _), _*) => {
        // If first value is true - add time between it and next value to time accumulator or it won't be accounted
        val firstGapMs = if (aVal) bTime.toMillis - aTime.toMillis else 0L
        val (overallMs, _) = queue.foldLeft((firstGapMs, aTime)) {
          case ((sumMs, prevTime), (nextTime, nextVal)) =>
            if (nextVal) (sumMs + nextTime.toMillis - prevTime.toMillis, nextTime)
            else (sumMs, nextTime)
        }
        overallMs
      }

      // if queue contains 1 or 0 elements
      case _ => 0L
    }
  }



  case class NumericAccumulatedState(window: Window, sum: Double = 0d, count: Long = 0l, queue: Queue[(Time, Double)] = Queue.empty)
       extends Serializable with AccumulatedStateLike[Double] {

    def updated(time: Time, value: Double): NumericAccumulatedState = {

      @tailrec
      def removeOldElementsFromQueue(as: NumericAccumulatedState): NumericAccumulatedState =
        as.queue.dequeueOption match {
          case Some(((oldTime, oldValue), newQueue)) if oldTime.plus(window) < time =>
            removeOldElementsFromQueue(
              NumericAccumulatedState(
                window = window,
                sum = sum - oldValue.toDouble(),
                count = count - 1,
                queue = newQueue
              )
            )
          case _ => as
        }

      val cleaned = removeOldElementsFromQueue(this)

      NumericAccumulatedState(window,
        sum = cleaned.sum + value.toDouble(),
        count = cleaned.count + 1,
        queue = cleaned.queue.enqueue((time, value)))
    }

    def avg: Double = {
      assert(count != 0, "Illegal state!") // it should n't be called on empty state
      sum / count
    }

    def startTime: Option[Time] = queue.headOption.map(_._1)

    def overallTimeMs: Long = {
      val timeOpt = for {
        startTime <- startTime
        (lastTime, _) <- queue.lastOption
      } yield lastTime.toMillis - startTime.toMillis
      timeOpt getOrElse 0L
    }
  }


  /**
    * PhaseParser collecting average value within window
    *
    * @param innerPhase - function to extract value of collecting field from event
    * @param accumulator - state to gather
    * @tparam AccumOut type of accumulating elements
    */
  case class AccumulationPhase[Event, InnerState, AccumOut, Out]
      (innerPhase: PhaseParser[Event, InnerState, AccumOut], window: Window, accumulator: AccumulatedStateLike[AccumOut])
      (extractResult: AccumulatedStateLike[AccumOut] => Out, extractorName: String)
      (implicit timeExtractor: TimeExtractor[Event])
    extends AggregatorPhases[Event, (InnerState, AccumulatedStateLike[AccumOut]), Out] {

    override def apply(event: Event, oldState: (InnerState, AccumulatedStateLike[AccumOut])):
      (PhaseResult[Out], (InnerState, AccumulatedStateLike[AccumOut])) =
    {
      val time = timeExtractor(event)
      val (oldInnerState, oldAverageState) = oldState
      val (innerResult, newInnerState) = innerPhase(event, oldInnerState)

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

    override def initialState: (InnerState, AccumulatedStateLike[AccumOut]) = innerPhase.initialState -> accumulator

    override def format(event: Event, state: (InnerState, AccumulatedStateLike[AccumOut])) = if (state._2.hasState) {
      s"$extractorName(${innerPhase.format(event, state._1)})=${extractResult(state._2)}"
    } else {
      s"$extractorName(${innerPhase.format(event, state._1)})"
    }
  }



  //todo MinParser, MaxParser, CountParser, MedianParser, ConcatParser, Timer

  case class PreviousValue[Event, State, Out](innerPhase: PhaseParser[Event, State, Out])
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
