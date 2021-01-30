package ru.itclover.tsp.dsl

import ru.itclover.tsp.core.Pattern.{Idx, IdxExtractor}
import ru.itclover.tsp.core.{RawPattern, Time}
import ru.itclover.tsp.core.io.{Decoder, Extractor, TimeExtractor}

// Helper method to extract only the used fields (identifiers) from the list of patterns
// Can use Any values.
@SuppressWarnings(Array("org.wartremover.warts.Any"))
object PatternFieldExtractor {

  def extract[E, EKey, EItem](patterns: Seq[RawPattern])(
    implicit fieldToEKey: Symbol => EKey
  ): Set[EKey] = {
    val dummyTimeExtractor = new TimeExtractor[E] {
      override def apply(e: E): Time = Time(0)
    }
    // Extracting nulls and converting to T.
    @SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.AsInstanceOf"))
    val dummyExtractor = new Extractor[E, EKey, EItem] {
      override def apply[T](e: E, k: EKey)(implicit d: Decoder[EItem, T]): T = null.asInstanceOf[T]
    }

    val dummyIdxExtractor = new IdxExtractor[E] {
      override def apply(e: E): Idx = 0L

      override def compare(x: Idx, y: Idx): Int = 0
    }

    val gen = ASTPatternGenerator[E, EKey, EItem]()(
      dummyIdxExtractor,
      dummyTimeExtractor,
      dummyExtractor,
      fieldToEKey
    )
    patterns
      .map { p =>
        gen
          .build(p.sourceCode, 0.0, 1000L, Map.empty)
          .map(_._2.fields.map(fieldToEKey))
          .getOrElse(Set.empty)
      }
      .foldLeft(Set.empty[EKey])(_ ++ _)
  }
}
