package ru.itclover.streammachine

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import ru.itclover.streammachine.core.PhaseParser
import ru.itclover.streammachine.core.PhaseResult.{Failure, Stay, Success}

import scala.reflect.ClassTag


/**
  * The function that maintains the per-IP-address state machines and verifies that the
  * events are consistent with the current state of the state machine. If the event is not
  * consistent with the current state, the function produces an alert.
  */
case class FlinkStateMachineMapper[Event, State: ClassTag, Out](phaseParser: PhaseParser[Event, State, Out]) extends RichFlatMapFunction[Event, Out] with Serializable {

  @transient
  private[this] var currentState: ValueState[State] = _

  override def open(config: Configuration): Unit = {
    val classTag = implicitly[ClassTag[State]]
    currentState = getRuntimeContext.getState(
      new ValueStateDescriptor("state", classTag.runtimeClass.asInstanceOf[Class[State]], phaseParser.initialState))
  }

  override def flatMap(t: Event, outCollector: Collector[Out]): Unit = {

    def processElement(restartIfFailed: Boolean): Unit = {

      val (result, newState) = phaseParser.apply(t, currentState.value())

      currentState.update(newState)

      result match {
        case Stay => // println(s"Stay for event $t")
        case Failure(msg) =>
          //todo Should we try to run this message again?
//          println(s"Fail ure for event $t: $msg")
          currentState.update(phaseParser.initialState)
          if (restartIfFailed) processElement(restartIfFailed = false)
        case Success(out) =>
//          println(s"Success for event $t")
          outCollector.collect(out)
      }
    }

    processElement(restartIfFailed = true)
  }

}
