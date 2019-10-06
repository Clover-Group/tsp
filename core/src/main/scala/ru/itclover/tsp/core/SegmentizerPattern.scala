package ru.itclover.tsp.core

import cats.{Foldable, Functor, Monad}
import cats.syntax.functor._
import ru.itclover.tsp.core.Pattern.QI

import scala.annotation.tailrec
import scala.languageFeature.higherKinds

//todo tests
//todo docs
case class SegmentizerPattern[Event, T, InnerState <: PState[T, InnerState]](inner: Pattern[Event, InnerState, T])
    extends Pattern[Event, InnerState, T] {

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
    oldState: InnerState,
    event: Cont[Event]
  ): F[InnerState] =
    inner(oldState, event).map(innerResult => {
      innerResult.queue.dequeueOption() match {
        case None               => innerResult
        case Some((head, tail)) => innerResult.copyWith(inner(tail, head, PQueue.empty))
      }
    })

  override def initialState(): InnerState = inner.initialState()
}
