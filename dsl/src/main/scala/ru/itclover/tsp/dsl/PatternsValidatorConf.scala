package ru.itclover.tsp.dsl
import ru.itclover.tsp.core.RawPattern

case class PatternsValidatorConf(patterns: Seq[RawPattern], fields: Map[String, String]) {}
