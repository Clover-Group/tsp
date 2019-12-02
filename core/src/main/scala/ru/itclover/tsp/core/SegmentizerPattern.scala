package ru.itclover.tsp.core

import cats.syntax.functor._
import cats.{Foldable, Functor, Monad}
import ru.itclover.tsp.core.Pattern.QI

import scala.annotation.tailrec

/*
Joins together sequential outputs of the inner pattern with the same value. It reduces amount of produced results.
 */
case class SegmentizerPattern[Event, T, InnerState](inner: Pattern[Event, InnerState, T])
    extends Pattern[Event, SegmentizerPState[InnerState, T], T] {
//todo tests

  @tailrec
  private def inner(q: QI[T], last: IdxValue[T], resultQ: QI[T]): QI[T] = {
    q.dequeueOption() match {
      case None => resultQ.enqueue(last)
      case Some((head, tail)) => {
        if (head.value.equals(last.value)) {
          inner(tail, last.copy(end = head.end), resultQ)
        } else {
          inner(tail, head, resultQ.enqueue(last))
        }
      }
    }
  }

  override def apply[F[_]: Monad, Cont[_]: Foldable: Functor](
    oldState: SegmentizerPState[InnerState, T],
    queue: PQueue[T],
    event: Cont[Event]
  ): F[(SegmentizerPState[InnerState, T], PQueue[T])] =
    inner(oldState.innerState, oldState.innerQueue, event).map {
      case (innerResult, innerQueue) => {
        innerQueue.dequeueOption() match {
          case None => oldState.copy(innerState = innerResult) -> queue
          case Some((head, tail)) =>
            val newQueue = inner(tail, head, queue) // do not inline!
            SegmentizerPState(innerResult, PQueue.empty[T]) -> newQueue
        }
      }
    }

  override def initialState(): SegmentizerPState[InnerState, T] = SegmentizerPState(inner.initialState(), PQueue.empty)
}

case class SegmentizerPState[InnerState, T](innerState: InnerState, innerQueue: QI[T])
