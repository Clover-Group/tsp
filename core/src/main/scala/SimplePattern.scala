package ru.itclover.tsp.v2
import cats.{Foldable, Functor, Monad}
import ru.itclover.tsp.v2.Pattern.{IdxExtractor, QI}
import ru.itclover.tsp.v2.Pattern.IdxExtractor._
import cats.syntax.functor._
import cats.syntax.foldable._
import ru.itclover.tsp.v2.IdxValue.IdxValueSimple
import scala.collection.{mutable => m}
import scala.language.higherKinds

// TODO Rename to FunctionP(attern)?
/** Simple Pattern */
class SimplePattern[Event: IdxExtractor, T](f: Event => Result[T]) extends Pattern[Event, SimplePState[T], T] {
  override def apply[F[_]: Monad, Cont[_]: Foldable: Functor](
    oldState: SimplePState[T],
    events: Cont[Event]
  ): F[SimplePState[T]] = {
    Monad[F].pure(SimplePState(events.map(e => IdxValue(e.index, f(e))).foldLeft(oldState.queue) {
      case (oldStateQ, b) => oldStateQ.enqueue(b) // .. style?
    }))
  }
  override def initialState(): SimplePState[T] = SimplePState(PQueue.empty)
}

case class SimplePState[T](override val queue: QI[T]) extends PState[T, SimplePState[T]] {
  override def copyWith(queue: QI[T]): SimplePState[T] = this.copy(queue = queue)
}

case class ConstPattern[Event: IdxExtractor, T](value: Result[T]) extends SimplePattern[Event, T](_ => value)
