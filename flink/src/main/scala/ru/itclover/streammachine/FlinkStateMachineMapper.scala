package ru.itclover.streammachine

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import ru.itclover.streammachine.core.{PhaseParser, PhaseResult}
import ru.itclover.streammachine.core.PhaseResult.{Failure, Stay, Success}

import scala.reflect.ClassTag


// Try resultsParser in AbstractStateMachineMapper
case class FlinkStateMachineMapper[Event, State: ClassTag, Out](phaseParser: PhaseParser[Event, State, Out]) extends
  RichFlatMapFunction[Event, Out]
  with AbstractStateMachineMapper[Event, State, Out]
  with Serializable {

  @transient
  private[this] var currentState: ValueState[Seq[State]] = _

  override def open(config: Configuration): Unit = {
    val classTag = implicitly[ClassTag[State]]
    currentState = getRuntimeContext.getState(
      new ValueStateDescriptor("state", classTag.runtimeClass.asInstanceOf[Class[Seq[State]]], Seq.empty))
  }

  override def flatMap(t: Event, outCollector: Collector[Out]): Unit = {

    val (firstEmitted, newStates) = process(t, currentState.value())

    currentState.update(newStates)

    firstEmitted.foreach {
      case Success(x, _) => outCollector.collect(x)
      case Failure(_) =>
    }

  }

}
