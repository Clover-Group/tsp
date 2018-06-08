package ru.itclover.streammachine.transformers

import org.apache.flink.configuration.Configuration
import ru.itclover.streammachine.core.{PhaseParser, PhaseResult, Time}
import ru.itclover.streammachine.core.PhaseResult.{Failure, Success}
import ru.itclover.streammachine.core.Time.TimeExtractor
import ru.itclover.streammachine.{AbstractPatternMapper, ResultMapper}
import ru.itclover.streammachine.core.PhaseResult.heartbeat


class FlinkPatternMapper[Event, PhaseState, PhaseOut, MapperOut](
  compilePhaseParser: ClassLoader => PhaseParser[Event, PhaseState, PhaseOut],
  resultsMapper: ResultMapper[Event, PhaseOut, MapperOut],
  eventsMaxGapMs: Long,
  emptyEvent: Event,
  isTerminalEvent: Event => Boolean
)(
  implicit timeExtractor: TimeExtractor[Event]
) extends RichStatefulFlatMapper[Event, (Seq[PhaseState], Event), MapperOut] with
          AbstractPatternMapper[Event, PhaseState, PhaseOut] with Serializable {

  override val initialState = (Seq.empty, emptyEvent)
  var phaseParser: PhaseParser[Event, PhaseState, PhaseOut] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    phaseParser = compilePhaseParser(getRuntimeContext.getUserCodeClassLoader)
  }

  override def apply(event: Event, stateAndPrevEvent: (Seq[PhaseState], Event)) = {
    val prevEvent = stateAndPrevEvent._2
    val (results, newStates) = if (doProcessOldState(event, prevEvent)) {
      process(event, stateAndPrevEvent._1)
    } else {
      process(event, Seq.empty) match { case (res, states) =>
        // Heartbeat here for SegmentResultsMapper to split segment if successes stream splitted
        (heartbeat +: res, phaseParser.initialState +: states)
      }
    }
    // Accumulated successes (failures logged in process fn) and new states
    (resultsMapper(event, results).foldLeft[List[MapperOut]] (Nil) {
      case (successes, Success(x)) => x :: successes
      case (successes, Failure(_)) => successes // Failures just dropped here
    }, (newStates, event))
  }

  /** Check is new event from same events time seq */
  def doProcessOldState(currEvent: Event, prevEvent: Event) = {
    if (prevEvent == emptyEvent) true
    else timeExtractor(currEvent).toMillis - timeExtractor(prevEvent).toMillis < eventsMaxGapMs
  }

  /** Is it last event in a stream? */
  override def isEventTerminal(event: Event) = isTerminalEvent(event)
}
