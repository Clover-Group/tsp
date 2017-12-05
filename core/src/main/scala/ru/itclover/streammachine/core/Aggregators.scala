package ru.itclover.streammachine.core

import java.time.Instant

import ru.itclover.streammachine.core.PhaseResult.{Failure, Stay, Success}

import scala.annotation.tailrec
import scala.collection.immutable.Queue
import TimeImplicits._
import ru.itclover.streammachine.core.Aggregators.Average

import scala.math.Numeric.Implicits._
import Ordering.Implicits._

trait AggregatingPhaseParser[Event, S] extends NumericPhaseParser[Event, S]

object AggregatingPhaseParser{

  trait TimeLikeExtractor[Event, Time] {
    def extractTime(e: Event)(implicit timeLike: TimeLike[Time])
  }

  type TimeExtractor[Event] = TimeLikeExtractor[Event, _]

  def avg[Event, S](numeric:NumericPhaseParser[Event,S])(implicit timeExtractor: TimeExtractor[Event]): Average[] = {

  }
}

object Aggregators {

  trait WithWindow[D] {
    def window: D
  }

  case class AverageState[Time: TimeLike, D: DurationLike, T: Numeric]
  (window: D, sum: Double = 0d, count: Long = 0l, queue: Queue[(Time, T)] = Queue.empty)
    extends WithWindow[D] {

    def updated(time: Time, value: T): AverageState[Time, D, T] = {

      @tailrec
      def removeOldElementsFromQueue(as: AverageState[Time, D, T]): AverageState[Time, D, T] =
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
    * @param timeF   - function to extract Time field from event
    * @param extract - function to extract value of collecting field from event
    * @param window  - window to collect points within
    * @tparam Event - events to process
    * @tparam Time  - Time type
    * @tparam D     - Duration type
    * @tparam T     - output type, used if phase successfully terminated
    */
  case class Average[Event, Time: TimeLike, D: DurationLike, T: Numeric](timeF: Event => Time, extract: Event => T, window: D) extends PhaseParser[Event, AverageState[Time, D, T], Double] with WithWindow[D] {

    override def apply(event: Event, oldState: AverageState[Time, D, T]): (PhaseResult[Double], AverageState[Time, D, T]) = {
      val time = timeF(event)
      val value = extract(event)

      val newState = oldState.updated(time, value)

      (newState.startTime match {
        case Some(startTime) if time >= startTime.plus(window) => Success(newState.result)
        case _ => Stay
      }) -> newState
    }

    override def initialState: AverageState[Time, D, T] = AverageState(window)
  }

  /**
    * PhaseParser collecting average value within window
    *
    * @param timeF   - function to extract Time field from event
    * @param extract - function to extract value of collecting field from event
    * @param window  - window to collect points within
    * @tparam Event - events to process
    * @tparam Time  - Time type
    * @tparam D     - Duration type
    * @tparam T     - output type, used if phase successfully terminated
    */
  case class Average2[Event, Time: TimeLike, D: DurationLike, T: Numeric](timeF: Event => Time, extract: Event => T, window: D) extends PhaseParser[Event, AverageState[Time, D, T], Double] with WithWindow[D] {

    override def apply(event: Event, oldState: AverageState[Time, D, T]): (PhaseResult[Double], AverageState[Time, D, T]) = {
      val time = timeF(event)
      val value = extract(event)

      val newState = oldState.updated(time, value)

      (newState.startTime match {
        case Some(startTime) if time >= startTime.plus(window) => Success(newState.result)
        case _ => Stay
      }) -> newState
    }

    override def initialState: AverageState[Time, D, T] = AverageState(window)
  }

  //todo MinParser, MaxParser, CountParser, MedianParser, ConcatParser, Timer

  /**
    * Timer parser. Returns:
    * Stay - if passed less than atLeastSeconds
    * Success - if passed time is between atLeastSeconds and atMaxSeconds
    * Failure - if passed more than atMaxSeconds.
    *
    * @param extract
    * @param atLeastSeconds
    * @param atMaxSeconds
    * @tparam Event - events to process
    */
  case class Timer[Event, Time: TimeLike](
                                           extract: Event => Time,
                                           atLeastSeconds: Int = 0,
                                           atMaxSeconds: Int = Int.MaxValue)
    extends PhaseParser[Event, Option[Time], (Time, Time)] {

    assert(atLeastSeconds >= 0 && atMaxSeconds >= 0 && atMaxSeconds > atLeastSeconds,
      s"Incorrect Timer configuration (atLeastSeconds: $atLeastSeconds, atMaxSeconds: $atMaxSeconds)")

    override def apply(event: Event, state: Option[Time]): (PhaseResult[(Time, Time)], Option[Time]) = {

      val eventTime = extract(event)

      state match {
        case None =>
          Stay -> Some(eventTime)
        case Some(startTime) =>
          val result = if (startTime.plus(atLeastSeconds * 1000l) < eventTime) Stay
          else if (startTime.plus(atMaxSeconds * 1000l) <= eventTime) Success(startTime -> eventTime)
          else Failure(s"Timeout expired at $eventTime")

          result -> state
      }
    }

    override def initialState: Option[Time] = None
  }

}
