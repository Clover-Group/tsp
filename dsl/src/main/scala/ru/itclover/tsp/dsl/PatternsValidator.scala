package ru.itclover.tsp.dsl
import ru.itclover.tsp.core.Pattern
import ru.itclover.tsp.core.Time.TimeExtractor
import ru.itclover.tsp.dsl.schema.RawPattern
import ru.itclover.tsp.phases.NumericPhases.SymbolNumberExtractor

object PatternsValidator {

  def validate[Event](
    patterns: Seq[RawPattern]
  )(
    implicit timeExtractor: TimeExtractor[Event],
    symbolNumberExtractor: SymbolNumberExtractor[Event]
  ): Seq[(RawPattern, Either[String, (Pattern[Event, _, _], PhaseMetadata)])] = {
    // Syntax parsing and pattern building
    patterns.map(p => (p, PhaseBuilder.build(p.sourceCode)))
    // TODO: Something else?
  }
}
