package ru.itclover.tsp.dsl

import ru.itclover.tsp.core.Pattern
import ru.itclover.tsp.core.Pattern.IdxExtractor
import ru.itclover.tsp.core.io.{Extractor, TimeExtractor}

class CachedAstPatternGenerator[Event, EKey, EItem]()(
  implicit idxExtractor: IdxExtractor[Event],
  timeExtractor: TimeExtractor[Event],
  extractor: Extractor[Event, EKey, EItem],
  @transient fieldToEKey: Symbol => EKey
) extends ASTPatternGenerator[Event, EKey, EItem]()(idxExtractor, timeExtractor, extractor, fieldToEKey) {

  override def generatePattern(ast: AST): Pattern[Event, AnyState[Any], Any] = ???
}
