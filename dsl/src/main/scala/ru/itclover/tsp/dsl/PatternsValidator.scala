package ru.itclover.tsp.dsl

import ru.itclover.tsp.core.{Pattern, RawPattern}
import ru.itclover.tsp.io.{Decoder, Extractor, TimeExtractor}

object PatternsValidator {

  def validate[Event](
    patterns: Seq[RawPattern]
  )(
    implicit timeExtractor: TimeExtractor[Event],
    toNumberExtractor: Extractor[Event, Int, Any],
    doubleDecoder: Decoder[Any, Double]
  ): Seq[(RawPattern, Either[String, (Pattern[Event, _, _], PatternMetadata)])] = {
    // Since it's only the validation, we don't need any tolerance fraction here.
    patterns.map(p => (p, PatternBuilder.build(p.sourceCode, SyntaxParser.testFieldsIdxMap, 0.0)))
  }
}
