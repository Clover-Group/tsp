// Common objects for Testing

package ru.itclover.tsp.core.fixtures

import ru.itclover.tsp.core.Pattern._
import ru.itclover.tsp.core.Time
import ru.itclover.tsp.core.io.TimeExtractor

// Dummy event
sealed case class Event[+A](ts: Long, idx: Idx, row: A, col: A)

object Event {
  // Dummy extractor
  implicit val extractor: IdxExtractor[Event[_]] = IdxExtractor.of(_.idx)

  implicit val timeExtractor: TimeExtractor[Event[_]] = TimeExtractor.of(t => Time(t.ts))
}

object Common {

  type EInt = Event[Int]

  val event: Event[Int] = Event[Int](0L, 0L, 0, 0)

  // Dummy event processing
  def procEvent(ev: EInt): Long = ev.row.toLong

}
