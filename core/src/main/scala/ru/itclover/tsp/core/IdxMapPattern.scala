package ru.itclover.tsp.core

import cats.syntax.functor._
import cats.{Foldable, Functor, Monad}
import ru.itclover.tsp.core.PQueue.IdxMapPQueue
import ru.itclover.tsp.core.Pattern.{QI, WithInners}

import scala.language.higherKinds

/**
  *
  * @param extractor special kind of extractor for Segments creation
  * @tparam E Event
  * @tparam Inner Inner result
  * @tparam S Holds State for the next step AND results (wrong named `queue`)
  */

class IdxMapPattern[Event, T1, T2, InnerState <: PState[T1, InnerState]](inner: Pattern[Event, InnerState, T1])(
  func: IdxValue[T1] => Result[T2]
) extends Pattern[Event, IdxMapPState[InnerState, T1, T2], T2]
    with WithInners[Event] {
  override def apply[F[_]: Monad, Cont[_]: Foldable: Functor](
    oldState: IdxMapPState[InnerState, T1, T2],
    event: Cont[Event]
  ): F[IdxMapPState[InnerState, T1, T2]] =
    inner.apply(oldState.innerState, event).map(innerResult => oldState.copy(innerState = innerResult))

  override def initialState(): IdxMapPState[InnerState, T1, T2] = IdxMapPState(innerState = inner.initialState(), func)
  override def innerPatterns: Seq[Pattern[Event, _, _]] = Seq(inner)
}

case class IdxMapPState[InnerState <: PState[T1, InnerState], T1, T2](
  innerState: InnerState,
  func: IdxValue[T1] => Result[T2]
) extends PState[T2, IdxMapPState[InnerState, T1, T2]] {
  override def queue: QI[T2] = IdxMapPQueue(innerState.queue, func)
  override def copyWith(queue: QI[T2]): IdxMapPState[InnerState, T1, T2] = {
    val prevSize = innerState.queue.size
    val toDrop = prevSize - queue.size
    assert(toDrop >= 0, "Illegal state, queue cannot grow in map")
    this.copy(innerState = innerState.copyWith(innerState.queue.drop(toDrop)))
  }
}
