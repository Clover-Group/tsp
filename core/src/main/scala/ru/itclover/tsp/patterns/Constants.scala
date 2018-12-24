package ru.itclover.tsp.patterns

import ru.itclover.tsp.core.PatternResult.{Failure, Stay, Success}
import ru.itclover.tsp.core.NoStatePattern
import ru.itclover.tsp.io.{Decoder, Extractor}
import scala.language.implicitConversions

object Constants {
  def const[Event, T](value: T) = ConstPattern[Event, T](value)

  case class ConstPattern[Event, +T](value: T) extends NoStatePattern[Event, T] {
    override def apply(e: Event, s: NoState) = (Success(value), s)

    override def format(event: Event, state: NoState) = value.toString
  }

  case class ExtractingPattern[Event, EKey, EItem, +T](key: EKey, keyName: Symbol)(
    implicit extract: Extractor[Event, EKey, EItem],
    decoder: Decoder[EItem, T]
  ) extends NoStatePattern[Event, T] {

    override def apply(e: Event, s: NoState) = {
      val x = extract(e, key)
      (if (x != null && x != Double.NaN) Success(x) else Stay) -> NoState.instance
    }

    override def format(event: Event, state: NoState) =
      keyName.toString.tail + "=" + extract(event, key).toString
  }

  case class FailurePattern[Event](msg: String) extends NoStatePattern[Event, Nothing] {
    override def apply(v1: Event, v2: NoState): (Failure, NoState) = Failure(msg) -> NoState.instance

    override def format(event: Event, state: NoState) = s"Failure($msg)"
  }
}
