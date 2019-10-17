package ru.itclover.tsp.mappers
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.Window
import org.apache.flink.util.Collector
import ru.itclover.tsp.core.PState

import scala.reflect.ClassTag

case class ProcessorCombinator[In, S <: PState[Inner, S]: ClassTag, Inner, Out](
  mappers: Seq[PatternProcessor[In, S, Inner, Out]]
) extends ProcessWindowFunction[In, Out, String, Window] {

  override def process(
    key: String,
    context: Context,
    elements: Iterable[In],
    out: Collector[Out]
  ): Unit = mappers.foreach(_.process( /*key,*/ elements.toList, out))

}
