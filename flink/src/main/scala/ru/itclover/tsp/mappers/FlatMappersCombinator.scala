package ru.itclover.tsp.mappers

import org.apache.flink.api.common.functions.{AbstractRichFunction, RichFlatMapFunction, RuntimeContext}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import scala.reflect.ClassTag


trait StatefulFlatMapper[In, State, Out] extends ((In, State) => (Seq[Out], State)) { self =>
  def initialState: State
}


class FlatMappersCombinator[In, S: ClassTag, Out](mappers: Seq[StatefulFlatMapper[In, S, Out]])
    extends RichFlatMapFunction[In, Out] {

  val mappersAndKeys = mappers.zipWithIndex

  private[this] var mappersStates: MapState[Int, S] = _

  override def open(config: Configuration): Unit = {
    super.open(config)
    val intClass: Class[Int] = 0.getClass.asInstanceOf[Class[Int]]
    val stateClass = implicitly[ClassTag[S]].runtimeClass.asInstanceOf[Class[S]]
    mappersStates = getRuntimeContext.getMapState(new MapStateDescriptor("States", intClass, stateClass))
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
}
