package ru.itclover.tsp.v2
import cats.{Foldable, Functor, Monad}
import ru.itclover.tsp.io.{Decoder, Extractor}
import ru.itclover.tsp.v2.Pattern.{IdxExtractor, QI}

class ExtractingPattern[Event: IdxExtractor, EKey, EItem, T, S <: PState[T, S], F[_]: Monad, Cont[_]: Functor: Foldable](
  key: EKey,
  keyName: Symbol
)(
  implicit extract: Extractor[Event, EKey, EItem],
  decoder: Decoder[EItem, T]
) extends Pattern[Event, T, S, F, Cont] {
  override def initialState(): S = ???
  override def apply(v1: S, v2: Cont[Event]): F[S] = ???
}

class ExtractingPState[T, S <: PState[T, S]] extends PState[T, ExtractingPState[T, S]] {
  override def queue: QI[T] = ???
  override def copyWithQueue(queue: QI[T]): ExtractingPState[T, S] = ???
}
