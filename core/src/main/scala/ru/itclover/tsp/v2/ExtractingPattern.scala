package ru.itclover.tsp.v2
import cats.{Foldable, Functor, Monad}
import ru.itclover.tsp.io.{Decoder, Extractor}
import ru.itclover.tsp.v2.Pattern.{IdxExtractor, QI}
import scala.language.higherKinds

class ExtractingPattern[Event: IdxExtractor, EKey, EItem, T, S <: PState[T, S], F[_]: Monad, Cont[_]: Functor: Foldable](
  key: EKey,
  keyName: Symbol
)(
  implicit extract: Extractor[Event, EKey, EItem],
  decoder: Decoder[EItem, T]
) extends SimplePattern[Event, T, F, Cont]({e =>
  val r = extract(e, key)
  Result.succ(r)
})
