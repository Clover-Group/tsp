package ru.itclover.tsp.io

import ru.itclover.tsp.core.Time

trait Extractor[Event, EKey, EItem] extends Serializable {
  // .. TODO Kind projector here
  def apply[T](e: Event, k: EKey)(implicit d: Decoder[EItem, T]): T
}

trait KVExtractor[Event, EKey, EItem] extends Serializable {
  // .. TODO Kind projector here
  def apply[T](e: Event, k: EKey): (EKey, EItem)
}


trait TimeExtractor[Event] extends Serializable {
  def apply(e: Event): Time
}


trait Extractors[Event, EKey, EItem] {
  def timeExtractor: TimeExtractor[Event]
  def anyExtractor: Extractor[Event, EKey, EItem]
  def indexNumberExtractor: Event => Double
}

trait KVExtractors[Event, EKey, EValue] {
  def keyValueExtractor: KVExtractor[Event, EKey, EValue]
}