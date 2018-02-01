package ru.itclover.streammachine.transformers

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import ru.itclover.streammachine.core.{PhaseParser, Time}
import ru.itclover.streammachine.core.PhaseResult.{Failure, Success}
import ru.itclover.streammachine.{AbstractStateMachineMapper, Eval, EvalUtils, ResultMapper}
import scala.reflect.ClassTag


// TODO: To Either - try to compile code in advance in companion
case class FlinkStateCodeMachineMapper[MapperOut](phaseIdAndCode: (String, String), fieldIndexesMap: Map[Symbol, Int],
                                                  mapResults: ResultMapper[Row, Any, MapperOut],
                                                  timestampField: Symbol)
  extends
    RichFlatMapFunction[Row, MapperOut] with AbstractStateMachineMapper[Row, Any, Any] with Serializable {
  require(fieldIndexesMap.nonEmpty)
  require(fieldIndexesMap.contains(timestampField))


  var phaseParser: PhaseParser[Row, Any, Any] = _

  @transient
  private[this] var currentState: ValueState[Seq[Any]] = _

  override def open(config: Configuration): Unit = {
    val classTag = implicitly[ClassTag[Any]]
    currentState = getRuntimeContext.getState(
      new ValueStateDescriptor("state", classTag.runtimeClass.asInstanceOf[Class[Seq[Any]]], Seq.empty))
    val evaluator = new Eval(getRuntimeContext.getUserCodeClassLoader)
    phaseParser = evaluator.apply[(PhaseParser[Row, Any, Any])](
      EvalUtils.composePhaseCodeUsingRowExtractors(phaseIdAndCode._2, timestampField, fieldIndexesMap)
    )
  }

  override def flatMap(event: Row, outCollector: Collector[MapperOut]): Unit = {
    val (results, newStates) = process(event, currentState.value())

    currentState.update(newStates)

    // TODO(r): collect with events and segmentate outside instead of mapResults
    mapResults(event, results).foreach {
      case Success(x) => outCollector.collect(x)
      case Failure(_) =>
    }

  }
}


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

}
