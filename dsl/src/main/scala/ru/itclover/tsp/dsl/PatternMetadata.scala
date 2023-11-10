package ru.itclover.tsp.dsl

import cats.kernel.Monoid

case class PatternMetadata(fields: Set[String], sumWindowsMs: Long)

object PatternMetadata {
  val empty = PatternMetadata(Set.empty, 0L)
}

object PatternMetadataInstances {

  implicit val monoid: Monoid[PatternMetadata] = new Monoid[PatternMetadata] {
    override def empty = PatternMetadata(Set.empty, 0L)

    override def combine(x: PatternMetadata, y: PatternMetadata) =
      PatternMetadata(x.fields ++ y.fields, x.sumWindowsMs + y.sumWindowsMs)

  }

}
