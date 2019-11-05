package ru.itclover.tsp.core
import cats.syntax.functor._
import cats.{Foldable, Functor, Monad}
import ru.itclover.tsp.core.PQueue.MapPQueue

import scala.language.higherKinds

case class MapPattern[Event, T1, T2, InnerState](inner: Pattern[Event, InnerState, T1])(val func: T1 => Result[T2])
    extends Pattern[Event, MapPState[InnerState, T1], T2] {
  override def apply[F[_]: Monad, Cont[_]: Foldable: Functor](
    oldState: MapPState[InnerState, T1],
    oldQueue: PQueue[T2],
    event: Cont[Event]
  ): F[(MapPState[InnerState, T1], PQueue[T2])] =
    inner(oldState.innerState, oldState.innerQueue, event).map {
      case (innerResult, innerQueue) => MapPState(innerState = innerResult, innerQueue) -> MapPQueue(innerQueue, func)
    }

  override def initialState(): MapPState[InnerState, T1] = MapPState(inner.initialState(), PQueue.empty)
}

case class MapPState[InnerState, T1](innerState: InnerState, innerQueue: PQueue[T1])
