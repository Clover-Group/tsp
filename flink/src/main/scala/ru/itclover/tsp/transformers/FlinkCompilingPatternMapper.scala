package ru.itclover.tsp.transformers

import akka.event.slf4j.Logger
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.{Counter, Gauge}
import ru.itclover.tsp.core.{Pattern, PatternResult, Time}
import ru.itclover.tsp.core.PatternResult.{Failure, Success}
import ru.itclover.tsp.io.TimeExtractor
import ru.itclover.tsp.{AbstractPatternMapper, ResultMapper}
import ru.itclover.tsp.core.PatternResult.heartbeat
import ru.itclover.tsp.phases.PatternStats
import ru.itclover.tsp.phases.TimeMeasurementPhases.TimeMeasurementPattern

class FlinkStatsPatternMapper[Event, PhaseState, PhaseOut, MapperOut](
                                                                  phase: TimeMeasurementPattern[Event, PhaseState, PhaseOut],
                                                                  resultsMapper: ResultMapper[Event, PhaseOut, MapperOut],
                                                                  eventsMaxGapMs: Long,
                                                                  emptyEvent: Event,
                                                                  isTerminalEvent: Event => Boolean
                                                                )(implicit timeExtractor: TimeExtractor[Event])
  extends FlinkCompilingPatternMapper[Event, (PhaseState, PatternStats), PhaseOut, MapperOut](
    ((_: ClassLoader) => phase.asInstanceOf[Pattern[Event, (PhaseState, PatternStats), PhaseOut]]),
    resultsMapper,
    eventsMaxGapMs,
    emptyEvent,
    isTerminalEvent
  ) {
  val logger = Logger("StatsMapper")
  //var stats = PatternStats(0L, 0L)

  var timeMetric: Option[Counter] = None
  var callsMetric: Option[Counter] = None

  private def setCounter(counter: Option[Counter], value: Long): Option[Unit] = {
    counter match {
      case Some(c) =>
        c.dec(c.getCount)
        c.inc(value)
        //logger.info(s"$c set to $value")
        Some(())
      case None =>
        sys.error("Counters not initialised!")
    }
  }

  override def open(parameters: _root_.org.apache.flink.configuration.Configuration): Unit = {
    super.open(parameters)
    timeMetric = Some(getRuntimeContext.getMetricGroup//.addGroup("PatternStats")
      .counter(s"PatternStats_${phase.patternId}_${Integer.toUnsignedLong(phase.patternName.hashCode)}_Time"))
    callsMetric = Some(getRuntimeContext.getMetricGroup//.addGroup("PatternStats")
      .counter(s"PatternStats_${phase.patternId}_${Integer.toUnsignedLong(phase.patternName.hashCode)}_Calls"))
  }

  override def apply(
    event: Event,
    stateAndPrevEvent: (scala.Seq[ (PhaseState, PatternStats)], Event)
  ): (scala.List[MapperOut], (_root_.scala.collection.Seq[ (PhaseState, PatternStats)], Event)) = {
    if (stateAndPrevEvent._1.nonEmpty) {
      //stats = stateAndPrevEvent._1.map(x => x._2).foldLeft(stats)(_ + _)
      //stats = stateAndPrevEvent._1.last._2
      val stats = stateAndPrevEvent._1.last._2
      setCounter(timeMetric, stats.time)
      setCounter(callsMetric, stats.calls)
    }
    val x = super.apply(event, stateAndPrevEvent)
    val (mapperOut, (newStatesAndStats, newEvent)) = x
    x
  }

  override def close(): Unit = {
    //logger.info(s"${phase.patternName} xxx $stats")
    super.close()
  }
}

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
