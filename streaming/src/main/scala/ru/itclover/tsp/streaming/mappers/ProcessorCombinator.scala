package ru.itclover.tsp.streaming.mappers

import java.util.concurrent.atomic.AtomicLong

import ru.itclover.tsp.RowWithIdx
import ru.itclover.tsp.core.io.TimeExtractor

case class ProcessorCombinator[In, S, Out](
                                                   mappers: Seq[PatternProcessor[In, S, Out]],
                                                   timeExtractor: TimeExtractor[In]
                                                 ) {

  private val counter = new AtomicLong(0)

  def process(elements: fs2.Chunk[In]): fs2.Chunk[Out] = {
    // todo hack!!!
    val sorted = elements.toList.toBuffer.sortBy(timeExtractor.apply)
    val processed = if (sorted.head.isInstanceOf[RowWithIdx]) {
      val indexed =
        sorted.map(x => x.asInstanceOf[RowWithIdx].copy(idx = counter.incrementAndGet())).asInstanceOf[Iterable[In]]
      mappers.flatMap(_.map(indexed))
    } else {
      mappers.flatMap(_.map(sorted))
    }
    fs2.Chunk(processed:_*)
  }

}
