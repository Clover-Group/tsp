package ru.itclover.tsp.spark.utils

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.sql.Row
import ru.itclover.tsp.core.Pattern.Idx
import ru.itclover.tsp.core.io.TimeExtractor

case class RowWithIdx(idx: Idx, row: Row)


case class ProcessorCombinator[In, S, Inner, Out](
  mappers: Seq[PatternProcessor[In, S, Out]],
  timeExtractor: TimeExtractor[In]
) {

  private val counter = new AtomicLong(0)

  def process(
    elements: Iterable[In],
  ): Iterable[Out] = {
    val sorted = elements.toBuffer.sortBy(timeExtractor.apply)
    if (sorted.head.isInstanceOf[RowWithIdx]) {
      val indexed =
        sorted.map(x => x.asInstanceOf[RowWithIdx].copy(idx = counter.incrementAndGet())).asInstanceOf[Iterable[In]]
      mappers.flatMap(_.process( /*key,*/ indexed))
    } else {
      mappers.flatMap(_.process( /*key,*/ sorted))
    }
  }

}
