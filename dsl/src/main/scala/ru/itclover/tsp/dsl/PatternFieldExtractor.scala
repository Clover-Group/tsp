package ru.itclover.tsp.dsl

import ru.itclover.tsp.core.Pattern.TsIdxExtractor
import ru.itclover.tsp.core.{RawPattern, Time}
import ru.itclover.tsp.core.io.{Decoder, Extractor, TimeExtractor}


// Helper method to extract only the used fields (identifiers) from the list of patterns
object PatternFieldExtractor {
  def extract[E, EKey, EItem](patterns: Seq[RawPattern])(
    implicit fieldToEKey: Symbol => EKey,
  ): Set[EKey] = {
    val dummyTimeExtractor = new TimeExtractor[E] {
      override def apply(e: E): Time = Time(0)
    }
    val dummyExtractor = new Extractor[E, EKey, EItem] {
      override def apply[T](e: E, k: EKey)(implicit d: Decoder[EItem, T]): T = null.asInstanceOf[T]
    }

    val tsToIdx = new TsIdxExtractor[E](dummyTimeExtractor(_).toMillis)
    val gen = ASTPatternGenerator[E, EKey, EItem]()(tsToIdx, dummyTimeExtractor, dummyExtractor, fieldToEKey, tsToIdx)
    patterns.map { p =>
      gen.build(p.sourceCode, 0.0, Map.empty)
        .map(_._2.fields.map(fieldToEKey))
        .getOrElse(Set.empty)
    }.foldLeft(Set.empty[EKey])(_ ++ _)
  }
}
