package ru.itclover.streammachine.transformers

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import org.apache.flink.api.common.functions.{RichFlatMapFunction, RuntimeContext}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import ru.itclover.streammachine.core.{PhaseParser, PhaseResult, Time}
import ru.itclover.streammachine.core.PhaseResult.{Failure, Success}
import ru.itclover.streammachine.core.Time.TimeExtractor
import ru.itclover.streammachine.{AbstractStateMachineMapper, ResultMapper}
import scala.reflect.ClassTag
import ru.itclover.streammachine.core.PhaseResult.heartbeat


class FlinkCompilingPattern[Event, PhaseState, PhaseOut, MapperOut](
  compilePhaseParser: ClassLoader => PhaseParser[Event, PhaseState, PhaseOut],
  resultsMapper: ResultMapper[Event, PhaseOut, MapperOut],
  eventsMaxGapMs: Long,
  emptyEvent: Event,
  isTerminal: Event => Boolean
)(
  implicit timeExtractor: TimeExtractor[Event]
) extends RichStatefulFlatMapper[Event, (Seq[PhaseState], Event), MapperOut] with
          AbstractStateMachineMapper[Event, PhaseState, PhaseOut] with Serializable {

  override val initialState = (Seq.empty, emptyEvent)
  var phaseParser: PhaseParser[Event, PhaseState, PhaseOut] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    phaseParser = compilePhaseParser(getRuntimeContext.getUserCodeClassLoader)
  }

  override def apply(event: Event, state: (Seq[PhaseState], Event)) = {
    val prevRow = state._2
    val (results, newStates) = if (doProcessOldState(event, prevRow)) {
      process(event, state._1)
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
  override def isEventTerminal(event: Event) = isTerminal(event)
}


case class FlinkPattern[Event, State: ClassTag, PhaseOut, MapperOut]
(phaseParser: PhaseParser[Event, State, PhaseOut])(
  mapResults: ResultMapper[Event, PhaseOut, MapperOut])
  extends
    RichFlatMapFunction[Event, MapperOut]
    with AbstractStateMachineMapper[Event, State, PhaseOut]
    with Serializable {

  @transient
  private[this] var currentState: ValueState[Seq[State]] = _

  override def open(config: Configuration): Unit = {
    val classTag = implicitly[ClassTag[State]]
    currentState = getRuntimeContext.getState(
      new ValueStateDescriptor("state", classTag.runtimeClass.asInstanceOf[Class[Seq[State]]], Seq.empty))
  }

  override def flatMap(event: Event, outCollector: Collector[MapperOut]): Unit = {
    if(currentState == null){
      val classTag = implicitly[ClassTag[State]]
      getRuntimeContext.getUserCodeClassLoader
      currentState = getRuntimeContext.getState(
        new ValueStateDescriptor("state", classTag.runtimeClass.asInstanceOf[Class[Seq[State]]], Seq.empty))
    }

    val (results, newStates) = process(event, currentState.value())

    currentState.update(newStates)

    mapResults(event, results).foreach {
      case Success(x) => outCollector.collect(x)
      case Failure(_) =>
    }

  }

  override def isEventTerminal(event: Event) = false // TODO
}
