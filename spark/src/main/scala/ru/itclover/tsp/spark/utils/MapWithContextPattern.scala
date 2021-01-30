package ru.itclover.tsp.spark.utils

import cats.syntax.foldable._
import cats.syntax.functor._
import cats.{Foldable, Functor, Monad}
import ru.itclover.tsp.core.PQueue.MapPQueue
import ru.itclover.tsp.core.Result._
import ru.itclover.tsp.core.{PQueue, Pattern, Result}

import scala.language.higherKinds

//todo tests
// MapWithContextPattern is a pattern which can add context to the results of inner using information from head of `events`.
case class MapWithContextPattern[Event, InnerState, T1, T2](inner: Pattern[Event, InnerState, T1])(
  val func: Event => T1 => T2
) extends Pattern[Event, InnerState, T2] {
  override def apply[F[_]: Monad, Cont[_]: Foldable: Functor](
    oldState: InnerState,
    oldQueue: PQueue[T2],
    event: Cont[Event]
  ): F[(InnerState, PQueue[T2])] = {

    // we need to trim inner queue here to avoid memory leaks
    val innerQueue = getInnerQueue(oldQueue)
    val headOption = event.get(0).toResult
    inner(oldState, innerQueue, event).map {
      case (innerResult, innerQueue) =>
        innerResult -> MapPQueue[T1, T2](innerQueue, _.value.flatMap(t1 => headOption.map(head => func(head)(t1))))
    }
  }

  private def getInnerQueue(queue: PQueue[T2]): PQueue[T1] = {
    queue match {
      case MapPQueue(innerQueue, _) if innerQueue.isInstanceOf[PQueue[T1]] => innerQueue.asInstanceOf[PQueue[T1]]
      //this is for first call
      case x if x.size == 0 => x.asInstanceOf[PQueue[T1]]
      case _                => sys.error("Wrong logic! MapPattern.apply must be called only with MapPQueue")
    }
  }

  override def initialState(): InnerState = inner.initialState()
}
