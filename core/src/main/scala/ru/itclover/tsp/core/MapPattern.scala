package ru.itclover.tsp.core
import cats.syntax.functor._
import cats.{Foldable, Functor, Monad}
import ru.itclover.tsp.core.PQueue.MapPQueue
import ru.itclover.tsp.core.Pattern.{QI, WithInner}

import scala.language.higherKinds

//todo optimize Map(Simple) => Simple
case class MapPattern[Event, T1, T2, InnerState <: PState[T1, InnerState]](val inner: Pattern[Event, InnerState, T1])(
 val func: T1 => Result[T2]
) extends Pattern[Event, MapPState[InnerState, T1, T2], T2] with WithInner[Event, InnerState,T1]{
  override def apply[F[_]: Monad, Cont[_]: Foldable: Functor](
    oldState: MapPState[InnerState, T1, T2],
    event: Cont[Event]
  ): F[MapPState[InnerState, T1, T2]] =
    inner(oldState.innerState, event).map({ innerResult =>
      oldState.copy(innerState = innerResult)
    })

  override def initialState(): MapPState[InnerState, T1, T2] = MapPState(innerState = inner.initialState(), func)
}

case class MapPState[InnerState <: PState[T1, InnerState], T1, T2](
  innerState: InnerState,
  func: T1 => Result[T2]
) extends PState[T2, MapPState[InnerState, T1, T2]] {
  override def queue: QI[T2] = MapPQueue(innerState.queue, func)
  override def copyWith(queue: QI[T2]): MapPState[InnerState, T1, T2] = {
    val prevSize = innerState.queue.size
    val toDrop = prevSize - queue.size
    assert(toDrop >= 0, "Illegal state, queue cannot grow in map")
    this.copy(innerState = innerState.copyWith(innerState.queue.drop(toDrop)))
  }
}
