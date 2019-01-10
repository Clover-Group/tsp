package ru.itclover.tsp.mappers

import cats.Id
import ru.itclover.tsp.io.TimeExtractor
import ru.itclover.tsp.v2.{Pattern, PState, StateMachine, Succ}


case class PatternFlatMapper[E, State <: PState[Inner, State], Inner, Out](
  pattern: Pattern[E, Inner, State, Id, List],
  mapResults: (E, Seq[Inner]) => Seq[Out],
  eventsMaxGapMs: Long,
  emptyEvent: E
)(
  implicit timeExtractor: TimeExtractor[E]
) extends StatefulFlatMapper[E, (State, E), Out]
    with Serializable {

  override def initialState = (pattern.initialState(), emptyEvent)

  override def apply(event: E, stateAndPrevEvent: (State, E)) = {
    val prevEvent = stateAndPrevEvent._2
    val newState: State = if (doProcessOldState(event, prevEvent)) {
      StateMachine.run(pattern, List(event), stateAndPrevEvent._1)
    } else {
      StateMachine.run(pattern, List(event), pattern.initialState()) // .. careful here, init state may need to create only 1 time
    }
    val results = newState.queue.map(_.value).collect { case Succ(v) => v }
    // Non failed results with events (for toIncidentsMapper) + state with previous event (to tract gaps in the data)
    (mapResults(event, results), (newState, event))
  }

  /** Check is new event from same events time seq */
  def doProcessOldState(currEvent: E, prevEvent: E) = {
    if (prevEvent == emptyEvent) true
    else timeExtractor(currEvent).toMillis - timeExtractor(prevEvent).toMillis < eventsMaxGapMs
  }
}

object PatternFlatMapper {
  val currentEventTsMetric = "currentEventTs"
}
