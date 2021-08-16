package ru.itclover.tsp.streaming.utils

import ru.itclover.tsp.core.io.TimeExtractor

import java.util.concurrent.atomic.AtomicLong

trait Indexer[EventWithIndex, +Event] {
  def isIndexed: Boolean
  def getUnindexedEvent(event: EventWithIndex): Event
}

case class ProcessorCombinator[In, S, Inner, Out](
  mappers: Seq[PatternProcessor[In, S, Out]],
  timeExtractor: TimeExtractor[In]
)(implicit indexer: Indexer[In, Any]) {

  private val counter = new AtomicLong(0)

  def process(
    elements: Iterable[In]
  ): Iterable[Out] = {
    val sorted = elements.toBuffer.sortBy(timeExtractor.apply)
    if (indexer.isIndexed) {
      val indexed =
        sorted.map(x => (counter.incrementAndGet(), indexer.getUnindexedEvent(x))).asInstanceOf[Iterable[In]]
      mappers.flatMap(_.process( /*key,*/ indexed))
    } else {
      mappers.flatMap(_.process( /*key,*/ sorted))
    }
  }

}
