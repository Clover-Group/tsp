package ru.itclover.tsp.core
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.{Foldable, Functor, Monad}
import ru.itclover.tsp.core.Pattern.{Idx, QI}

import scala.annotation.tailrec
import scala.language.higherKinds

/** AndThen  */
//We lose T1 and T2 in output for performance reason only. If needed outputs of first and second stages can be returned as well
case class AndThenPattern[Event, T1, T2, S1, S2](first: Pattern[Event, S1, T1], second: Pattern[Event, S2, T2])
    extends Pattern[Event, AndThenPState[T1, T2, S1, S2], (Idx, Idx)] {

  def apply[F[_]: Monad, Cont[_]: Foldable: Functor](
    oldState: AndThenPState[T1, T2, S1, S2],
    oldQueue: PQueue[(Idx, Idx)],
    event: Cont[Event]
  ): F[(AndThenPState[T1, T2, S1, S2], PQueue[(Idx, Idx)])] = {

    val firstF = first.apply[F, Cont](oldState.firstState, oldState.firstQueue, event)
    val secondF = second.apply[F, Cont](oldState.secondState, oldState.secondQueue, event)

    for (newFirstOutput  <- firstF;
         newSecondOutput <- secondF)
      yield {
        // process queues
        val (updatedFirstQueue, updatedSecondQueue, finalQueue) =
          process(newFirstOutput._2, newSecondOutput._2, oldQueue)

        AndThenPState(
          newFirstOutput._1,
          updatedFirstQueue,
          newSecondOutput._1,
          updatedSecondQueue
        ) -> finalQueue
      }
  }

  override def initialState(): AndThenPState[T1, T2, S1, S2] =
    AndThenPState(first.initialState(), PQueue.empty, second.initialState(), PQueue.empty)

  private def process(firstQ: QI[T1], secondQ: QI[T2], totalQ: QI[(Idx, Idx)]): (QI[T1], QI[T2], QI[(Idx, Idx)]) = {

    @tailrec
    def inner(first: QI[T1], second: QI[T2], total: QI[(Idx, Idx)]): (QI[T1], QI[T2], QI[(Idx, Idx)]) = {

      def default: (QI[T1], QI[T2], QI[(Idx, Idx)]) = (first, second, total)

      (first.headOption, second.headOption) match {
        case (None, _) => default
        case (_, None) => default
        case (Some(IdxValue(start1, end1, value1)), Some(IdxValue(start2, end2, value2))) =>
          if (value1.isFail) {
            inner(
              first.behead(),
              PQueue.unwindWhile(second)(_.end <= start1),
              total.enqueue(IdxValue(start1, end1, Result.fail))
            )
          } else if (value2.isFail) {
            // Do not return Fail for the first part yet, unless it is the end of the queue
            first.size match {
              case 1 => inner(first.rewindTo(end2 + 1), second.behead(), total.enqueue(IdxValue(start1, end2, Fail)))
              case _ => inner(first, second.behead(), total)}
          } else { // at this moment both first and second results are not Fail.
            // late event from second, just skip it and fail this part only.
            // first            |-------|
            // second  |------|
            if (start1 > end2) {
              inner(first, second.behead(), total.enqueue(IdxValue(start2, end2, Fail)))
            }
            // Gap between first and second. Just behead first and fail this part only.
            // first   |-------|
            // second             |------|
            else if (end1 + 1 < start2) {
              inner(first.behead(), second, total.enqueue(IdxValue(start1, end1, Fail)))
            }
            // First and second intersect
            // first   |-------|
            // second       |-------|
            // result  |------------| (take union, not intersection)
            else {
              val end = Math.max(end1 + 1, end2)
              val start = Math.min(start1, start2)
              val newResult = IdxValue(start, end, Succ((start, end))) // todo nobody uses the output of AndThen pattern. Let's drop it later.
              inner(first.rewindTo(end + 1), second.rewindTo(end + 1), total.enqueue(newResult))
            }
          }
      }

    }

    inner(firstQ, secondQ, totalQ)
  }

  override val patternTag: PatternTag = AndThenPatternTag
}

case class AndThenPState[T1, T2, State1, State2](
  firstState: State1,
  firstQueue: PQueue[T1],
  secondState: State2,
  secondQueue: PQueue[T2]
)
