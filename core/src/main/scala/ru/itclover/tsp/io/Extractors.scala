package ru.itclover.tsp.io

import ru.itclover.tsp.core.Time

trait Extractor[Event, EKey, EItem] extends Serializable {
  // .. TODO Kind projector here
  def apply[T](e: Event, k: EKey)(implicit d: Decoder[EItem, T]): T
}


trait TimeExtractor[Event] extends Serializable {
  def apply(e: Event): Time
}


trait Extractors[Event] {
  def timeExtractor: TimeExtractor[Event]
  def indexNumberExtractor: Event => Double
}
