// Common objects for Testing

package ru.itclover.tsp.core

import ru.itclover.tsp.core.Pattern._
import ru.itclover.tsp.core.io.TimeExtractor

// Dummy event
sealed case class Event[A](ts: Long, row: A, col: A)

final object Common {

  type EInt = Event[Int]

  val event = Event[Int](0L, 0, 0)

  // Dummy event processing
  def procEvent(ev: EInt): Long = ev.row

  // Dummy extractor
  implicit val extractor: TsIdxExtractor[EInt] = new TsIdxExtractor(procEvent)

  implicit val timeExtractor: TimeExtractor[EInt] = TimeExtractor.of(t => Time(t.ts))
}
