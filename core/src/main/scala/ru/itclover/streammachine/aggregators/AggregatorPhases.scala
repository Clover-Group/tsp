package ru.itclover.streammachine.aggregators

import scala.Ordering.Implicits._
import scala.annotation.tailrec
import scala.collection.{mutable => m}
import scala.math.Numeric.Implicits._
import ru.itclover.streammachine.core.PhaseResult.{Failure, Stay, Success}
import ru.itclover.streammachine.core.Time._
import ru.itclover.streammachine.utils.CollectionsOps.MutableQueueOps
import ru.itclover.streammachine.core.{PhaseParser, PhaseResult, Time, Window}
import ru.itclover.streammachine.phases.BooleanPhases.BooleanPhaseParser
import ru.itclover.streammachine.phases.CombiningPhases.{And, TogetherParserLike}
import ru.itclover.streammachine.phases.NumericPhases.NumericPhaseParser
import ru.itclover.streammachine.phases.TimePhases.Wait


trait AggregatorPhases[Event, S, T] extends PhaseParser[Event, S, T]

object AggregatorPhases {

  trait AggregatorFunctions {

    def avg[Event, S](numeric: PhaseParser[Event, S, Double], window: Window)
                     (implicit timeExtractor: TimeExtractor[Event]): AccumulationPhase[Event, S, Double, Double] = {
      AccumulationPhase(numeric, NumericAccumulatedState(window), window)({ case a: NumericAccumulatedState => a.avg }, "avg")
    }

    def sum[Event, S, T](numeric: NumericPhaseParser[Event, S], window: Window)
                        (implicit timeExtractor: TimeExtractor[Event]): AccumulationPhase[Event, S, Double, Double] =
      AccumulationPhase(numeric, NumericAccumulatedState(window), window)({ case a: NumericAccumulatedState => a.sum }, "sum")

    def count[Event, S, T](phase: PhaseParser[Event, S, T], window: Window)
                          (implicit timeExtractor: TimeExtractor[Event]): AccumulationPhase[Event, S, T, Long] = {
      AccumulationPhase(phase, CountAccumulatedState[T](window), window)({ case a: CountAccumulatedState[T] => a.count }, "count")
    }

    def millisCount[Event, S, T](phase: PhaseParser[Event, S, T], window: Window)
                                (implicit timeExtractor: TimeExtractor[Event]): AccumulationPhase[Event, S, T, Long] =
      AccumulationPhase(phase, CountAccumulatedState[T](window), window)({ case a: CountAccumulatedState[T] => a.overallTimeMs.getOrElse(0L) }, "millisCount")


    def truthCount[Event, S](boolean: BooleanPhaseParser[Event, S], window: Window)
                            (implicit timeExtractor: TimeExtractor[Event]): AccumulationPhase[Event, S, Boolean, Long] = {
      AccumulationPhase(boolean, TruthAccumulatedState(window), window)({ case a: TruthAccumulatedState => a.truthCount }, "truthCount")
    }

    def truthMillisCount[Event, S](boolean: BooleanPhaseParser[Event, S], window: Window)
                                  (implicit timeExtractor: TimeExtractor[Event]): AccumulationPhase[Event, S, Boolean, Long] = {
      AccumulationPhase(boolean, TruthAccumulatedState(window), window)({ case a: TruthAccumulatedState => a.truthMillisCount }, "truthMillisCount")
    }


    def lag[Event, S, Out](phase: PhaseParser[Event, S, Out]) = PreviousValue(phase)

    /*def lag[Event, S, Out](phase: PhaseParser[Event, S, Out], window: Window)
                          (implicit timeExtractor: TimeExtractor[Event]) =
      first(phase, window)

    def first[Event, S, T](phase: PhaseParser[Event, S, T], window: Window)
                          (implicit timeExtractor: TimeExtractor[Event]): AccumulationPhase[Event, S, T, T] = {
      AccumulationPhase(phase, CountAccumulatedState[T](window), window)({ case a: CountAccumulatedState[T] => a.queue.head._2 }, "first")
    }*/

    def delta[Event, S](numeric: NumericPhaseParser[Event, S])
                       (implicit timeExtractor: TimeExtractor[Event]): NumericPhaseParser[Event, _] =
      numeric minus PreviousValue(numeric)

    def deltaMillis[Event, S, T](phase: PhaseParser[Event, S, T])
                                (implicit timeExtractor: TimeExtractor[Event]): NumericPhaseParser[Event, _] =
      CurrentTimeMs(phase) minus PreviousTimeMs(phase)


  }

  case class Segment(from: Time, to: Time) extends Serializable // TODO to packege object

  type ValueAndTime = Double And Time


  trait AccumulatedState[T] extends Serializable { // To sep file
    def window: Window

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

  case class AccumulationPhase[Event, InnerState, AccumOut, Out]
      (innerPhase: PhaseParser[Event, InnerState, AccumOut], window: Window, accumulator: () => AccumulatedState[AccumOut])
      (extractResult: AccumulatedState[AccumOut] => Out, extractorName: String)
      (implicit timeExtractor: TimeExtractor[Event])
    extends AggregatorPhases[Event, (InnerState, AccumulatedState[AccumOut]), Out] {

    case class CountState(startTime: Time, count: Int)

    val optState = (innerPhase.initialState)

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



  //todo MinParser, MaxParser, MedianParser, ConcatParser, Timer

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
        case x@Stay => x
        case Failure(x) => Failure(x)
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
