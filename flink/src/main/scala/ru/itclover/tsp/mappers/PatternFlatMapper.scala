package ru.itclover.tsp.mappers

import ru.itclover.tsp.core.{Pattern, PatternResult, Time}
import ru.itclover.tsp.core.PatternResult.{heartbeat, Failure, Success, TerminalResult}
import ru.itclover.tsp.io.TimeExtractor
import ru.itclover.tsp.{AbstractPatternMapper, ResultMapper}


case class PatternFlatMapper[E, State, Inner, Out](
  pattern: Pattern[E, State, Inner],
  mapResults: (E, Seq[TerminalResult[Inner]]) => Seq[Out],
  eventsMaxGapMs: Long,
  emptyEvent: E,
  isTerminalEvent: E => Boolean
)(
  implicit timeExtractor: TimeExtractor[E]
) extends StatefulFlatMapper[E, (Seq[State], E), Out]
    with AbstractPatternMapper[E, State, Inner]
    with Serializable {

  override def initialState = (Seq.empty, emptyEvent)

  override def apply(event: E, stateAndPrevEvent: (Seq[State], E)) = {
    val prevEvent = stateAndPrevEvent._2
    val (results, newStates) = if (doProcessOldState(event, prevEvent)) {
      process(event, stateAndPrevEvent._1)
    } else {
      process(event, Seq.empty) match { case (res, states) =>
        // Heartbeat here for SegmentResultsMapper to split segment if successes stream splitted
        (heartbeat +: res, pattern.initialState +: states)
      }
    }
    // Terminal results with events (for toIncidentsMapper) + state with previous event (to tract gaps in the data)
    (mapResults(event, results), (newStates, event))
  }

  /** Check is new event from same events time seq */
  def doProcessOldState(currEvent: E, prevEvent: E) = {
    if (prevEvent == emptyEvent) true
    else timeExtractor(currEvent).toMillis - timeExtractor(prevEvent).toMillis < eventsMaxGapMs
  }

  /** Is it last event in a stream? */
  override def isEventTerminal(event: E) = isTerminalEvent(event)
}

object PatternFlatMapper {
  val currentEventTsMetric = "currentEventTs"
}
