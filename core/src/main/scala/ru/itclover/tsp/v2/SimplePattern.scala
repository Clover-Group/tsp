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
class SimplePattern[Event: IdxExtractor, T, F[_]: Monad, Cont[_]: Functor: Foldable](f: Event => Result[T])
    extends Pattern[Event, T, SimplePState[T], F, Cont] {
  override def apply(oldState: SimplePState[T], events: Cont[Event]): F[SimplePState[T]] = {
    Monad[F].pure(SimplePState(events.map { e => IdxValueSimple(e.index, f(e)) }.foldLeft(oldState.queue) {
      case (oldStateQ, b) => { oldStateQ.enqueue(b); oldStateQ } // .. style?
    }))
  }
  override def initialState(): SimplePState[T] = SimplePState(m.Queue.empty)
}

case class SimplePState[T](override val queue: QI[T]) extends PState[T, SimplePState[T]] {
  override def copyWithQueue(queue: QI[T]): SimplePState[T] = this.copy(queue = queue)
}

case class ConstPattern[Event: IdxExtractor, T, F[_]: Monad, Cont[_]: Functor: Foldable](value: T)
    extends SimplePattern[Event, T, F, Cont](_ => Result.succ(value))
