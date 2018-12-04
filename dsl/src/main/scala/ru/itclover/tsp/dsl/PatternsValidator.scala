/*
package ru.itclover.tsp.dsl

import ru.itclover.tsp.core.Pattern
import ru.itclover.tsp.dsl.schema.RawPattern
import ru.itclover.tsp.io.TimeExtractor

object PatternsValidator {

  def validate[Event](
    patterns: Seq[RawPattern]
  )(
    implicit timeExtractor: TimeExtractor[Event],
    toNumberExtractor: Extractor[Event]
  ): Seq[(RawPattern, Either[String, (Pattern[Event, _, _], PhaseMetadata)])] = {
    patterns.map(p => (p, PhaseBuilder.build(p.sourceCode, SyntaxParser.testFieldsIdxMap)))
  }
}
*/
