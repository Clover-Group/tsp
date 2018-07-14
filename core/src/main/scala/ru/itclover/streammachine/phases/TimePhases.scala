package ru.itclover.streammachine.phases

import ru.itclover.streammachine.core.PhaseParser.WithParser
import ru.itclover.streammachine.core.PhaseResult.{Failure, Stay, Success}
import ru.itclover.streammachine.core.Time.{MaxWindow, MinWindow, TimeExtractor}
import ru.itclover.streammachine.core._
import ru.itclover.streammachine.phases.BooleanPhases.BooleanPhaseParser
import ru.itclover.streammachine.phases.CombiningPhases.{TogetherParserLike}

import scala.Ordered._

object TimePhases {

  trait TimePhasesSyntax[Event, State, T] {
    this: WithParser[Event, State, T] =>

    def timed(timeInterval: TimeInterval)(implicit timeExtractor: TimeExtractor[Event]): Timed[Event, State, T] =
      Timed(this.parser, timeInterval)

    def timed(min: Window = MinWindow, max: Window = MaxWindow)(implicit timeExtractor: TimeExtractor[Event]): Timed[Event, State, T] =
      timed(TimeInterval(min, max))

  }

  /**
    * Timer parser. Returns:
    * Stay - if passed less than min boundary of timeInterval
    * Success - if passed time is between time interval
    * Failure - if passed more than max boundary of timeInterval
    *
    * @param timeInterval  - time limits
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
          val lowerBound = startTime.plus(timeInterval.min)
          val upperBound = startTime.plus(timeInterval.max)
          val result = if (eventTime < lowerBound) Stay
          else if (eventTime <= upperBound) Success(startTime -> eventTime)
          else Failure(s"Timeout expired at $eventTime")

          result -> state
      }
    }

    /**
      * We override this method here because of don't want to start timer eagely.
      *
      * @param event
      * @param state
      * @return
      */
    override def aggregate(event: Event, state: Option[Time]): Option[Time] = state

    override def initialState: Option[Time] = None
  }


  case class Timed[Event, State, Out](inner: PhaseParser[Event, State, Out], timeInterval: TimeInterval)
                                     (implicit timeExtractor: TimeExtractor[Event])
       extends TogetherParserLike(inner, Timer(timeInterval)) {

    override def format(e: Event, state: (State, Option[Time])) =
      s"(${inner.format(e, state._1)}).timed(${timeInterval.min}, ${timeInterval.max})" +
        state._2.map(t => s"=$t").getOrElse("")
  }

}
