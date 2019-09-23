package ru.itclover.tsp.core
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.{Foldable, Functor, Monad, Order}
import ru.itclover.tsp.core.Pattern.{Idx, QI}

import scala.annotation.tailrec
import scala.language.higherKinds

/** AndThen  */
//We lose T1 and T2 in output for performance reason only. If needed outputs of first and second stages can be returned as well
case class AndThenPattern[Event, T1, T2, S1 <: PState[T1, S1], S2 <: PState[T2, S2]](
  first: Pattern[Event, S1, T1],
  second: Pattern[Event, S2, T2]
)(
  implicit idxOrd: Order[Idx]
) extends Pattern[Event, AndThenPState[T1, T2, S1, S2], (Idx, Idx)] {

  def apply[F[_]: Monad, Cont[_]: Foldable: Functor](
    oldState: AndThenPState[T1, T2, S1, S2],
    event: Cont[Event]
  ): F[AndThenPState[T1, T2, S1, S2]] = {

    val firstF = first.apply[F, Cont](oldState.first, event)
    val secondF = second.apply[F, Cont](oldState.second, event)

    for (newFirstState  <- firstF;
         newSecondState <- secondF)
      yield {
        // process queues
        val (updatedFirstQueue, updatedSecondQueue, finalQueue) =
          process(newFirstState.queue, newSecondState.queue, oldState.queue)

        AndThenPState(
          newFirstState.copyWith(updatedFirstQueue),
          newSecondState.copyWith(updatedSecondQueue),
          finalQueue
        )
      }
  }

  override def initialState(): AndThenPState[T1, T2, S1, S2] =
    AndThenPState(first.initialState(), second.initialState(), PQueue.empty)

  private def process(firstQ: QI[T1], secondQ: QI[T2], totalQ: QI[(Idx, Idx)]): (QI[T1], QI[T2], QI[(Idx, Idx)]) = {

    @tailrec
    def inner(first: QI[T1], second: QI[T2], total: QI[(Idx, Idx)]): (QI[T1], QI[T2], QI[(Idx, Idx)]) = {

      def default: (QI[T1], QI[T2], QI[(Idx, Idx)]) = (first, second, total)

      (first.headOption, second.headOption) match {
        case (None, _) => default
        case (_, None) => default
        case (Some(iv1 @ IdxValue(start1, end1, value1)), Some(iv2 @ IdxValue(start2, end2, value2))) =>
          if (value1.isFail) {
            inner(first.behead(), second, total.enqueue(IdxValue(start1, end1, Result.fail)))
          } else if (value2.isFail) {
            val newFirst = iv1.removeBefore(end2 + 1).map(_ => first.rewindTo(end2 + 1)).getOrElse(first.behead())
            inner(newFirst, second, total.enqueue(IdxValue(start1, end2, Fail)))
          } else { // at this moment both first and second results are not Fail.
            // late event from second, just skip it
            // first            |-------|
            // second  |------|
            if (idxOrd.gt(start1, end2)) {
              inner(first, second.behead(), total)
            }
            // Gap between first and second. Just behead first
            // first   |-------|
            // second             |------|
            else if (idxOrd.lt(end1 + 1, start2)) {
              inner(first.behead(), second, total)
            }
            // First and second intersect
            // first   |-------|
            // second       |-------|
            else {
              val end = Math.min(end1, end2)
              val start = Math.max(start1, start2)
              val newResult = IdxValue(start, end, Succ(start, end)) // todo nobody uses the output of AndThen pattern. Let's drop it later.
              inner(first.rewindTo(end + 1), second.rewindTo(end + 1), total.enqueue(newResult))
            }
          }
      }

    }

    inner(firstQ, secondQ, totalQ)
  }
}

case class AndThenPState[T1, T2, State1 <: PState[T1, State1], State2 <: PState[T2, State2]](
  first: State1,
  second: State2,
  override val queue: QI[(Idx, Idx)]
) extends PState[(Idx, Idx), AndThenPState[T1, T2, State1, State2]] {

  override def copyWith(queue: QI[(Idx, Idx)]): AndThenPState[T1, T2, State1, State2] = this.copy(queue = queue)
}

object AndThenPState {}
