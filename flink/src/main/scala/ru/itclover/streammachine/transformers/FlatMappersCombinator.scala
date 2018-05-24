package ru.itclover.streammachine.transformers

import org.apache.flink.api.common.functions.{AbstractRichFunction, RichFlatMapFunction, RuntimeContext}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import ru.itclover.streammachine.core.PhaseResult.{Failure, Success}
import scala.reflect.ClassTag


class FlatMappersCombinator[In, State: ClassTag, Out](mappers: Seq[RichStatefulFlatMapper[In, State, Out]])
    extends RichFlatMapFunction[In, Out] {

  val mappersAndKeys = mappers.zipWithIndex

  private[this] var mappersStates: MapState[Int, State] = _

  override def open(config: Configuration): Unit = {
    super.open(config)
    val intClass: Class[Int] = 0.getClass.asInstanceOf[Class[Int]]
    val stateClass = implicitly[ClassTag[State]].runtimeClass.asInstanceOf[Class[State]]
    mappersStates = getRuntimeContext.getMapState(new MapStateDescriptor("States", intClass, stateClass))
    mappersAndKeys foreach { case (mapper, _) => mapper.open(config) }
  }

  override def setRuntimeContext(t: RuntimeContext): Unit = {
    super.setRuntimeContext(t)
    mappers.foreach(_.setRuntimeContext(t))
  }

  override def flatMap(value: In, out: Collector[Out]): Unit = {
    val resultsAndStates = mappersAndKeys map {
      case (mapper, key) =>
        val state = mappersStates.get(key)
        mapper(value, if (state != null) state else mapper.initialState)
    }

    resultsAndStates.zipWithIndex foreach { case ((results, newStates), key) =>
        results.foreach(out.collect)
        mappersStates.put(key, newStates)
    }
  }

  override def close(): Unit = mappers.foreach(_.close())
}
