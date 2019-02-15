package ru.itclover.tsp.mappers
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.Window
import org.apache.flink.util.Collector
import ru.itclover.tsp.mappers.PatternProcessor
import ru.itclover.tsp.v2.PState

import scala.reflect.ClassTag
//
//import org.apache.flink.api.common.functions.{AbstractRichFunction, RichFlatMapFunction, RuntimeContext}
//import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor}
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.util.Collector
//import scala.reflect.ClassTag
//
//
//trait StatefulFlatMapper[In, State, Out] extends ((In, State) => (Seq[Out], State)) with Serializable { self =>
//  def initialState: State
//}
//
//
//class FlatMappersCombinator[In, S: ClassTag, Out](mappers: Seq[StatefulFlatMapper[In, S, Out]])
//    extends RichFlatMapFunction[In, Out] {
//
//  val mappersAndKeys = mappers.zipWithIndex
//
//  private[this] var mappersStates: MapState[Int, S] = _
//
//  override def open(config: Configuration): Unit = {
//    super.open(config)
//    val intClass: Class[Int] = 0.getClass.asInstanceOf[Class[Int]]
//    val stateClass = implicitly[ClassTag[S]].runtimeClass.asInstanceOf[Class[S]]
//    mappersStates = getRuntimeContext.getMapState(new MapStateDescriptor("States", intClass, stateClass))
//  }
//
//  override def flatMap(value: In, out: Collector[Out]): Unit = {
//    val resultsAndStates = mappersAndKeys map {
//      case (mapper, key) =>
//        val state = mappersStates.get(key)
//        mapper(value, if (state != null) state else mapper.initialState)
//    }
//
//    resultsAndStates.zipWithIndex foreach { case ((results, newStates), key) =>
//        results.foreach(out.collect)
//        mappersStates.put(key, newStates)
//    }
//  }
//}

case class ProcessorCombinator[In, S <: PState[Inner, S]: ClassTag, Inner, Out](mappers: Seq[PatternProcessor[In, S, Inner, Out]])
    extends ProcessWindowFunction[In, Out, String, Window] {
  var counter = 0
  var chunkCounter = 0

override def process(
    key: String,
    context: Context,
    elements: Iterable[In],
    out: Collector[Out]
  ): Unit = {
    val list = elements.toList
    chunkCounter += 1
    counter += list.length
    println(s"Processing chunk #$chunkCounter of ${list.length} events, total $counter")
    mappers.foreach(_.process(key, list, out))
  }
}
