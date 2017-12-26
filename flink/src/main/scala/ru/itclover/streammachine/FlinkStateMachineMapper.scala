package ru.itclover.streammachine

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import ru.itclover.streammachine.core.{PhaseParser, PhaseResult, Time}
import ru.itclover.streammachine.core.PhaseResult.{Failure, Success}
import scala.reflect.ClassTag


case class FlinkStateMachineMapper[Event, State: ClassTag, PhaseOut, MapperOut]
(phaseParser: PhaseParser[Event, State, PhaseOut],
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

    val (results, newStates) = process(event, currentState.value())

    currentState.update(newStates)

    mapResults(event, results).foreach {
      case Success(x) => outCollector.collect(x)
      case Failure(_) =>
    }

  }

}
