package ru.itclover.tsp.v2
import cats.{Functor, Monad}
import ru.itclover.tsp.v2.Extract.{IdxExtractor, QI, Result}
import ru.itclover.tsp.v2.Extract.IdxExtractor._
import cats.syntax.functor._

import scala.collection.{mutable => m}
import scala.language.higherKinds

/** Simple Pattern */
class SimplePattern[Event: IdxExtractor, T, F[_]: Monad, Cont[_]: Functor: AddToQueue](f: Event => Result[T])
    extends Pattern[Event, T, SimplePState[T], F, Cont] {
  override def apply(oldState: SimplePState[T], events: Cont[Event]): F[SimplePState[T]] = {
    val addToQueue: AddToQueue[Cont] = implicitly[AddToQueue[Cont]]
    Monad[F].pure(SimplePState(addToQueue.addToQueue(events.map(e => IdxValue(e.index, f(e))), oldState.queue)))
  }
  override def initialState(): SimplePState[T] = SimplePState(m.Queue.empty)
}

case class SimplePState[T](override val queue: QI[T]) extends PState[T, SimplePState[T]] {
  override def copyWithQueue(queue: QI[T]): SimplePState[T] = this.copy(queue = queue)
}

case class ConstPattern[Event: IdxExtractor, T, F[_]: Monad, Cont[_]: Functor: AddToQueue](t: T)
    extends SimplePattern[Event, T, F, Cont](_ => Result.succ(t))
