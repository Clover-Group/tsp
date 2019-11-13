package ru.itclover.tsp.core
import cats.syntax.foldable._
import cats.syntax.functor._
import cats.{Foldable, Functor, Monad}
import ru.itclover.tsp.core.Pattern.IdxExtractor._
import ru.itclover.tsp.core.Pattern.{IdxExtractor, QI}

import scala.language.higherKinds

// TODO Rename to FunctionP(attern)?
/** Simple Pattern */
trait SimplePatternLike[Event, T] extends Pattern[Event, SimplePState[T], T] {
  def idxExtractor: IdxExtractor[Event]
  val f: Event => Result[T]

  override def apply[F[_]: Monad, Cont[_]: Foldable: Functor](
    oldState: SimplePState[T],
    events: Cont[Event]
  ): F[SimplePState[T]] =
    Monad[F].pure(SimplePState(events.map(e => IdxValue(e.index(idxExtractor), f(e))).foldLeft(oldState.queue) {
      case (oldStateQ, b) => oldStateQ.enqueue(b) // .. style?
    }))

}

case class SimplePattern[Event: IdxExtractor, T](override val f: Event => Result[T])
    extends SimplePatternLike[Event, T] {
  override def initialState(): SimplePState[T] = SimplePState(PQueue.empty)

  override def idxExtractor: IdxExtractor[Event] = implicitly
}

case class SimplePState[T](override val queue: QI[T]) extends PState[T, SimplePState[T]] {
  override def copyWith(queue: QI[T]): SimplePState[T] = this.copy(queue = queue)
}

case class ConstPattern[Event: IdxExtractor, T](value: Result[T]) extends SimplePatternLike[Event, T] {
  override val f: Event => Result[T] = _ => value
  override def initialState(): SimplePState[T] = SimplePState(PQueue.empty)

  override def idxExtractor: IdxExtractor[Event] = implicitly
}
