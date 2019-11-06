package ru.itclover.tsp.mappers
import java.util.concurrent.atomic.AtomicLong

import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.Window
import org.apache.flink.util.Collector
import ru.itclover.tsp.RowWithIdx
import ru.itclover.tsp.core.io.TimeExtractor

import scala.collection.GenSeq
import scala.reflect.ClassTag

case class ProcessorCombinator[In, S: ClassTag, Inner, Out](
  mappers: GenSeq[PatternProcessor[In, S, Inner, Out]],
  timeExtractor: TimeExtractor[In]
) extends ProcessWindowFunction[In, Out, String, Window] {

  private val counter = new AtomicLong(0)

  override def process(
    key: String,
    context: Context,
    elements: Iterable[In],
    out: Collector[Out]
  ): Unit = {
// todo hack!!!
    val sorted = elements.toBuffer.sortBy(timeExtractor.apply)
    if (sorted.head.isInstanceOf[RowWithIdx]) {
      val indexed =
        sorted.map(x => x.asInstanceOf[RowWithIdx].copy(idx = counter.incrementAndGet())).asInstanceOf[Iterable[In]]
      mappers.foreach(_.process( /*key,*/ indexed, out))
    } else {
      mappers.foreach(_.process( /*key,*/ sorted, out))
    }
  }

}
