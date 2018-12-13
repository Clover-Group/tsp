package ru.itclover.tsp.v2
import cats.{Functor, Monad}
import cats.syntax.functor._
import ru.itclover.tsp.v2.Extract.{QI, Result}

import scala.collection.immutable.Queue
import scala.language.higherKinds
//
////todo optimize Map(Simple) => Simple
//class MapPattern[Event, T1, T2, InnerState <: PState[T1, InnerState], F[_]: Monad, Cont[_]: Functor: AddToQueue](
//  inner: Pattern[Event, T1, InnerState, F, Cont]
//)(func: T1 => Result[T2])
//    extends Pattern[Event, T2, MapPState[InnerState, T1, T2], F, Cont] {
//  override def apply(
//    oldState: MapPState[InnerState, T1, T2],
//    event: Cont[Event]
//  ): F[MapPState[InnerState, T1, T2]] =
//    inner(oldState.innerState, event).map(innerResult => oldState.copy(innerState = innerResult))
//
//  override def initialState(): MapPState[InnerState, T1, T2] = MapPState(innerState = inner.initialState(), func)
//}
//
//case class MapPState[InnerState <: PState[T1, InnerState], T1, T2](
//  innerState: InnerState,
//  func: T1 => Result[T2]
//) extends PState[T2, MapPState[InnerState, T1, T2]] {
//  override def queue: QI[T2] = innerState.queue.map(tv => IdxValue(index = tv.index, value = tv.value.flatMap(func)))
//  override def copyWithQueue(queue: QI[T2]): MapPState[InnerState, T1, T2] = {
//    //todo .size on Queue is O(N) operation
//    val prevSize = innerState.queue.size
//    val toDrop = prevSize - queue.size
//    assert(toDrop >= 0, "Illegal state, queue cannot grow in map")
//    this.copy(innerState = innerState.copyWithQueue(innerState.queue.drop(toDrop)))
//  }
//
//}

/** Map Pattern */

//todo optimize Map(Simple) => Simple
class MapPattern[Event, T1, T2, InnerState <: PState[T1, InnerState], F[_]: Monad, Cont[_]: Functor: AddToQueue](
  inner: Pattern[Event, T1, InnerState, F, Cont]
)(func: T1 => Result[T2])
    extends Pattern[Event, T2, MapPState[InnerState, T1, T2], F, Cont] {
  override def apply(
    oldState: MapPState[InnerState, T1, T2],
    event: Cont[Event]
  ): F[MapPState[InnerState, T1, T2]] = {
    val innerF = inner(oldState.innerState, event)
    for (innerResult <- innerF)
      yield {
        val newQueue = innerResult.queue.map(tv => IdxValue(index = tv.index, value = tv.value.flatMap(func)))
        MapPState(innerResult.copyWithQueue(Queue.empty), oldState.queue ++ newQueue)
      }
  }
  override def initialState(): MapPState[InnerState, T1, T2] =
    MapPState(innerState = inner.initialState(), queue = Queue.empty)
}

case class MapPState[InnerState <: PState[T1, InnerState], T1, T2](
  innerState: InnerState,
  override val queue: QI[T2]
) extends PState[T2, MapPState[InnerState, T1, T2]] {
  override def copyWithQueue(queue: QI[T2]): MapPState[InnerState, T1, T2] = this.copy(queue = queue)
}
