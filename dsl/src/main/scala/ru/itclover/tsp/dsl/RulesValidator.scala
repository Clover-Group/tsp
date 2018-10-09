package ru.itclover.tsp.dsl
import ru.itclover.tsp.core.Pattern

object RulesValidator {
  def validate(rules: Seq[String]): Seq[Either[String, (Pattern[Nothing, _, _], PhaseMetadata)]] = {
    // Syntax parsing and pattern building
    rules.map(rule => PhaseBuilder.build(rule))
    // TODO: Something else?
  }
}
