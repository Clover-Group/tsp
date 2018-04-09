package ru.itclover.streammachine.transformers

import org.apache.flink.api.common.functions.{RichFlatMapFunction, RuntimeContext}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import ru.itclover.streammachine.core.{PhaseParser, Time}
import ru.itclover.streammachine.core.PhaseResult.{Failure, Success}
import ru.itclover.streammachine.core.Time.TimeExtractor
import ru.itclover.streammachine.{AbstractStateMachineMapper, ResultMapper}
import scala.reflect.ClassTag


case class FlinkStateCodeMachineMapper[MapperOut](compilePhaseParser: ClassLoader => PhaseParser[Row, Any, Any],
                                                  resultsMapper: ResultMapper[Row, Any, MapperOut],
                                                  eventsMaxGapMs: Long, isRowTerminal: Row => Boolean)
                                                 (implicit timeExtractor: TimeExtractor[Row])
  extends
    RichFlatMapFunction[Row, MapperOut] with AbstractStateMachineMapper[Row, Any, Any] with Serializable {

  var phaseParser: PhaseParser[Row, Any, Any] = _
  val emptyRow = new Row(0)

  @transient
  private[this] var stateAndPrevRow: ValueState[(Seq[Any], Row)] = _

  override def open(config: Configuration): Unit = {
    val classTag = implicitly[ClassTag[Any]]
    stateAndPrevRow = getRuntimeContext.getState(
      new ValueStateDescriptor("state", classTag.runtimeClass.asInstanceOf[Class[(Seq[Any], Row)]], (Seq.empty, emptyRow)))
    phaseParser = compilePhaseParser(getRuntimeContext.getUserCodeClassLoader)
  }

  override def flatMap(event: Row, outCollector: Collector[MapperOut]): Unit = {
    val (results, newStates) = process(event, stateAndPrevRow.value()._1)

    stateAndPrevRow.update((newStates, event))

    resultsMapper(event, results) foreach {
      case Success(x) => outCollector.collect(x)
      case Failure(_) =>
    }
  }


  override def close() = {
    super.close()
  }

  /** Check is new event from same events time seq */
  override def doProcessOldState(event: Row) = {
    val prevRow = stateAndPrevRow.value()._2
    if (prevRow == emptyRow) true
    else timeExtractor(event).toMillis - timeExtractor(prevRow).toMillis < eventsMaxGapMs
  }

  /** Check is event terminal. If true - drop state and return Failure for this event. */
  override def isEventTerminal(r: Row) = isRowTerminal(r)
}


case class FlinkStateMachineMapper[Event, State: ClassTag, PhaseOut, MapperOut]
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

  /** @inheritdoc */
  override def doProcessOldState(event: Event): Boolean = true // TODO

  override def isEventTerminal(event: Event) = false // TODO
}
