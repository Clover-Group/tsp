package ru.itclover.tsp.v2

import scala.language.higherKinds
import cats.{Functor, Monad}
import ru.itclover.tsp.v2.Pattern.QI

class SkipPattern[Event, T, InnerState <: PState[T, InnerState], F[_]: Monad, Cont[_]: Functor](
  inner: Pattern[Event, T, InnerState, F, Cont],
  eventCount: Int
) {
//  override def apply(
//    oldState: SkipPState[InnerState, T],
//    event: Cont[Event]
//  ): F[SkipPState[InnerState, T]] =
//    inner(oldState.innerState, event).map(innerResult => oldState.copy(innerState = innerResult))
}

case class SkipPState[InnerState <: PState[T, InnerState], T](innerState: InnerState, eventCount: Int)
    extends PState[T, SkipPState[InnerState, T]] {
  override def queue: QI[T] = innerState.queue.drop(eventCount)

  override def copyWithQueue(queue: QI[T]): SkipPState[InnerState, T] = {
    val prevSize = innerState.queue.size
    val toDrop = prevSize - queue.size
    assert(toDrop == eventCount, "Illegal state, drop count must be equal to the number of skipped events")
    this.copy(innerState = innerState.copyWithQueue(innerState.queue.drop(toDrop)))
  }
}
