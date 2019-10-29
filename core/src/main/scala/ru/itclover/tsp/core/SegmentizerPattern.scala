package ru.itclover.tsp.core

import cats.syntax.functor._
import cats.{Foldable, Functor, Monad}
import ru.itclover.tsp.core.Pattern.QI

import scala.annotation.tailrec

/*
Joins together sequential outputs of the inner pattern with the same value. It reduces amount of produced results.
 */
case class SegmentizerPattern[Event, T, InnerState <: PState[T, InnerState]](inner: Pattern[Event, InnerState, T])
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
    event: Cont[Event]
  ): F[SegmentizerPState[InnerState, T]] =
    inner(oldState.innerState, event).map(innerResult => {
      innerResult.queue.dequeueOption() match {
        case None => SegmentizerPState(innerResult, oldState.queue)
        case Some((head, tail)) =>
          val newQueue = inner(tail, head, oldState.queue) // do not inline!
          SegmentizerPState(innerResult.copyWith(PQueue.empty), newQueue)
      }
    })

  override def initialState(): SegmentizerPState[InnerState, T] = SegmentizerPState(inner.initialState(), PQueue.empty)
}

case class SegmentizerPState[InnerState, T](innerState: InnerState, override val queue: QI[T])
    extends PState[T, SegmentizerPState[InnerState, T]] {
  override def copyWith(queue: QI[T]): SegmentizerPState[InnerState, T] = this.copy(queue = queue)
}
