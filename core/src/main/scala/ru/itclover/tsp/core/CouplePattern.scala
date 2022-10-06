package ru.itclover.tsp.core

import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.{Foldable, Functor, Monad, Order}
import ru.itclover.tsp.core.Pattern.{Idx, IdxExtractor, QI}

import scala.annotation.tailrec

/** Couple Pattern */
case class CouplePattern[Event: IdxExtractor, State1, State2, T1, T2, T3](
  left: Pattern[Event, State1, T1],
  right: Pattern[Event, State2, T2]
)(
  val func: (Result[T1], Result[T2]) => Result[T3]
)(
  implicit idxOrd: Order[Idx] // ???
) extends Pattern[Event, CouplePState[State1, State2, T1, T2], T3] {
  override def apply[F[_]: Monad, Cont[_]: Foldable: Functor](
    oldState: CouplePState[State1, State2, T1, T2],
    oldQueue: PQueue[T3],
    events: Cont[Event]
  ): F[(CouplePState[State1, State2, T1, T2], PQueue[T3])] = {
    val leftF = left.apply(oldState.leftState, oldState.leftQueue, events)
    val rightF = right.apply(oldState.rightState, oldState.rightQueue, events)
    for (newLeftOutput  <- leftF;
         newRightOutput <- rightF) yield {
      // Build a new queue from the left and right ones
      val (updatedLeftQueue, updatedRightQueue, newFinalQueue) =
        processQueues(newLeftOutput._2, newRightOutput._2, oldQueue)

      CouplePState(newLeftOutput._1, updatedLeftQueue, newRightOutput._1, updatedRightQueue) -> newFinalQueue
    }
  }

  // todo test
  private def processQueues(firstQ: QI[T1], secondQ: QI[T2], totalQ: QI[T3]): (QI[T1], QI[T2], QI[T3]) = {

    @tailrec
    def inner(first: QI[T1], second: QI[T2], total: QI[T3]): (QI[T1], QI[T2], QI[T3]) = {
      def default: (QI[T1], QI[T2], QI[T3]) = (first, second, total)

      (first.headOption, second.headOption) match {
        // if any of parts is empty -> do nothing
        case (_, None)                                                                => default
        case (None, _)                                                                => default
        case (Some(IdxValue(start1, end1, val1)), Some(IdxValue(start2, end2, val2))) =>
          // we emit result only if results on left and right sides come at the same time
          if (idxOrd.eqv(start1, start2)) {
            val result: Result[T3] = func(val1, val2)
            val minEnd = idxOrd.min(end1, end2)

            val newStart = minEnd + 1
            inner(first.rewindTo(newStart), second.rewindTo(newStart), total.enqueue(IdxValue(start1, minEnd, result)))
          } else {
            // otherwise skip results from one of sides
            val cutTo = idxOrd.max(start1, start2)
            inner(first.rewindTo(cutTo), second.rewindTo(cutTo), total)
          }
      }
    }

    inner(firstQ, secondQ, totalQ)
  }

  override def initialState(): CouplePState[State1, State2, T1, T2] =
    CouplePState(left.initialState(), PQueue.empty, right.initialState(), PQueue.empty)
}

case class CouplePState[State1, State2, T1, T2](
  leftState: State1,
  leftQueue: PQueue[T1],
  rightState: State2,
  rightQueue: PQueue[T2]
)
