package ru.itclover.tsp.dsl
import ru.itclover.tsp.core.Pattern
import ru.itclover.tsp.core.Time.TimeExtractor
import ru.itclover.tsp.phases.NumericPhases.SymbolNumberExtractor

object PatternsValidator {

  def validate[Event](
    patterns: Seq[String]
  )(
    implicit timeExtractor: TimeExtractor[Event],
    symbolNumberExtractor: SymbolNumberExtractor[Event]
  ): Seq[(String, Either[String, (Pattern[Event, _, _], PhaseMetadata)])] = {
    // Syntax parsing and pattern building
    patterns.map(p => (p, PhaseBuilder.build(p)))
    // TODO: Something else?
  }
}
