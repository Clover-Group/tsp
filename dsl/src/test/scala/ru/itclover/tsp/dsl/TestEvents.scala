package ru.itclover.tsp.dsl

import ru.itclover.tsp.core.Time
import ru.itclover.tsp.core.Pattern.{Idx, IdxExtractor}
import ru.itclover.tsp.core.io.{Decoder, Extractor, TimeExtractor}

object TestEvents {
  case class TestEvent(
    time: Long,
    intSensor: Int,
    longSensor: Long,
    boolSensor: Boolean,
    doubleSensor1: Double,
    doubleSensor2: Double
  )

  implicit val timeExtractor: TimeExtractor[TestEvent] = (e: TestEvent) => Time(e.time)
  implicit val idxExtractor: IdxExtractor[TestEvent] = new IdxExtractor[TestEvent] {
    override def apply(e: TestEvent): Idx = e.time
    override def compare(x: Idx, y: Idx): Int = x.compare(y)
  }
  implicit val extractor: Extractor[TestEvent, Symbol, Any] = new Extractor[TestEvent, Symbol, Any] {
    override def apply[T](e: TestEvent, k: Symbol)(
      implicit d: Decoder[Any, T]
    ): T = d(k match {
      case 'intSensor     => e.intSensor
      case 'longSensor    => e.longSensor
      case 'boolSensor    => e.boolSensor
      case 'doubleSensor1 => e.doubleSensor1
      case 'doubleSensor2 => e.doubleSensor2
      case _              => null
    })
  }
}
