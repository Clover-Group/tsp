package ru.itclover.tsp.core
import ru.itclover.tsp.core.Pattern.IdxExtractor
import ru.itclover.tsp.core.io.{Decoder, Extractor}

import scala.language.higherKinds

class ExtractingPattern[Event: IdxExtractor, EKey, EItem, T, S <: PState[T, S]]
(key: EKey, keyName: Symbol)
(
  implicit extract: Extractor[Event, EKey, EItem],
  decoder: Decoder[EItem, T]
) extends SimplePattern[Event, T]({ e =>
      val r = extract(e, key)
      Result.succ(r)
    })
