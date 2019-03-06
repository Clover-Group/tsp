package ru.itclover.tsp.v2
import ru.itclover.tsp.io.{Decoder, Extractor}
import ru.itclover.tsp.v2.Pattern.IdxExtractor

import scala.language.higherKinds

class ExtractingPattern[Event: IdxExtractor, EKey, EItem, T, S <: PState[T, S]](key: EKey, keyName: Symbol)(
  implicit extract: Extractor[Event, EKey, EItem],
  decoder: Decoder[EItem, T]
) extends SimplePattern[Event, T]({ e =>
      val r = extract(e, key)
      Result.succ(r)
    })
