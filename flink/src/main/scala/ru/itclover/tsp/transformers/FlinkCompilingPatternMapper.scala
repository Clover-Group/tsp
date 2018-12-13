package ru.itclover.tsp.transformers

import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.{Counter, Gauge}
import ru.itclover.tsp.core.{Pattern, PatternResult, Idx}
import ru.itclover.tsp.core.PatternResult.{Failure, Success}
import ru.itclover.tsp.io.TimeExtractor
import ru.itclover.tsp.{AbstractPatternMapper, ResultMapper}
import ru.itclover.tsp.core.PatternResult.heartbeat

// .. TODO: Fix inheritance
class FlinkPatternMapper[Event, PhaseState, PhaseOut, MapperOut](
  phase: Pattern[Event, PhaseState, PhaseOut],
  resultsMapper: ResultMapper[Event, PhaseOut, MapperOut],
  eventsMaxGapMs: Long,
  emptyEvent: Event,
  isTerminalEvent: Event => Boolean
)(implicit timeExtractor: TimeExtractor[Event])
    extends FlinkCompilingPatternMapper[Event, PhaseState, PhaseOut, MapperOut](
      ((_: ClassLoader) => phase),
      resultsMapper,
      eventsMaxGapMs,
      emptyEvent,
      isTerminalEvent
    ) {}

// .. TODO: Mb set concrete ToFoundRuleResultMapper
class FlinkCompilingPatternMapper[Event, PhaseState, PhaseOut, MapperOut](
  compilePhaseParser: ClassLoader => Pattern[Event, PhaseState, PhaseOut],
  resultsMapper: ResultMapper[Event, PhaseOut, MapperOut],
  eventsMaxGapMs: Long,
  emptyEvent: Event,
  isTerminalEvent: Event => Boolean
)(
  implicit timeExtractor: TimeExtractor[Event]
) extends RichStatefulFlatMapper[Event, (Seq[PhaseState], Event), MapperOut]
    with AbstractPatternMapper[Event, PhaseState, PhaseOut]
    with Serializable {

  override val initialState = (Seq.empty, emptyEvent)
  var phaseParser: Pattern[Event, PhaseState, PhaseOut] = _


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
    (resultsMapper(event, results).foldLeft[List[MapperOut]](Nil) {
      case (successes, Success(x)) => x :: successes
      case (successes, Failure(_)) => successes // Failures just dropped here
    }, (newStates, event))
  }

  override def close(): Unit = {
    super.close()
    phaseParser = null
  }

  /** Check is new event from same events time seq */
  def doProcessOldState(currEvent: Event, prevEvent: Event) = {
    if (prevEvent == emptyEvent) true
    else timeExtractor(currEvent).toMillis - timeExtractor(prevEvent).toMillis < eventsMaxGapMs
  }

  /** Is it last event in a stream? */
  override def isEventTerminal(event: Event) = isTerminalEvent(event)
}

object FlinkCompilingPatternMapper {
  val currentEventTsMetric = "currentEventTs"
}
