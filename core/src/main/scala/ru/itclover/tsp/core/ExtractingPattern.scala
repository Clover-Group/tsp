package ru.itclover.tsp.core
import ru.itclover.tsp.core.Pattern.IdxExtractor
import ru.itclover.tsp.core.io.{Decoder, Extractor}

class ExtractingPattern[Event: IdxExtractor, EKey, EItem, T, S](key: EKey)(
  implicit extract: Extractor[Event, EKey, EItem],
  decoder: Decoder[EItem, T]
) extends SimplePattern[Event, T](e => {
      val r = extract(e, key)
      Result.succ(r)
    }) {
  override def toString: String = s"ExtractingPattern($key)"
}
