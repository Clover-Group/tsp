package ru.itclover.tsp.core.io

import ru.itclover.tsp.core.Time

trait Extractor[Event, EKey, EItem] extends Serializable {
  // TODO Kind projector here
  def apply[T](e: Event, k: EKey)(implicit d: Decoder[EItem, T]): T

  def comap[A](f: A => Event): Extractor[A, EKey, EItem] = {
    def thisApply[T](e: Event, k: EKey)(d: Decoder[EItem, T]) = apply(e, k)(d)
    new Extractor[A, EKey, EItem] {
      override def apply[T](e: A, k: EKey)(implicit d: Decoder[EItem, T]): T = thisApply(f(e), k)(d)
    }
  }

}

trait KVExtractor[Event, EKey, EItem] extends Serializable {
  // .. TODO Kind projector here
  def apply[T](e: Event, k: EKey): (EKey, EItem)
}

trait TimeExtractor[Event] extends Serializable {
  def apply(e: Event): Time

  def comap[A](f: A => Event): TimeExtractor[A] = TimeExtractor.of(f.andThen(apply))
}

object TimeExtractor {

  implicit class GetTime[T](private val event: T) extends AnyVal {
    def time(implicit te: TimeExtractor[T]): Time = te.apply(event)
  }

  def of[E](f: E => Time): TimeExtractor[E] = new TimeExtractor[E] {
    override def apply(e: E): Time = f(e)
  }

}

trait Extractors[Event] {
  def timeExtractor: TimeExtractor[Event]
  def indexNumberExtractor: Event => Double
}
