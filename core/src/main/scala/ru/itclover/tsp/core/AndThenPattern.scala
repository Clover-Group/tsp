package ru.itclover.tsp.core
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.foldable._
import cats.{Foldable, Functor, Monad}
import ru.itclover.tsp.core.AndThenPattern.TimeMap
import ru.itclover.tsp.core.Pattern.{Idx, IdxExtractor, QI}
import ru.itclover.tsp.core.aggregators.{TimerPattern, WaitPattern}
import ru.itclover.tsp.core.io.TimeExtractor._
import ru.itclover.tsp.core.Pattern.IdxExtractor._
import ru.itclover.tsp.core.io.TimeExtractor

import scala.collection.{mutable => m}
import scala.annotation.tailrec
import scala.language.higherKinds

/** AndThen  */
//We lose T1 and T2 in output for performance reason only. If needed outputs of first and second stages can be returned as well
case class AndThenPattern[Event: IdxExtractor: TimeExtractor, T1, T2, S1, S2](
  first: Pattern[Event, S1, T1],
  second: Pattern[Event, S2, T2]
) extends Pattern[Event, AndThenPState[T1, T2, S1, S2], (Idx, Idx)] {

  def apply[F[_]: Monad, Cont[_]: Foldable: Functor](
    oldState: AndThenPState[T1, T2, S1, S2],
    oldQueue: PQueue[(Idx, Idx)],
    event: Cont[Event]
  ): F[(AndThenPState[T1, T2, S1, S2], PQueue[(Idx, Idx)])] = {

    val firstF = first.apply[F, Cont](oldState.firstState, oldState.firstQueue, event)
    val secondF = second.apply[F, Cont](oldState.secondState, oldState.secondQueue, event)

    // populate TimeMap with idx->time tuples for new events
    val idxTimeMapWithNewEvents =
      event.foldLeft(oldState.indexTimeMap) { case (a, b) => a.enqueue(b.index -> b.time); a }

    for (newFirstOutput  <- firstF;
         newSecondOutput <- secondF)
      yield {
        // process queues
        val (updatedFirstQueue, updatedSecondQueue, finalQueue, updatedTimeMap) =
          process(newFirstOutput._2, newSecondOutput._2, oldQueue, idxTimeMapWithNewEvents)

        AndThenPState(
          newFirstOutput._1,
          updatedFirstQueue,
          newSecondOutput._1,
          updatedSecondQueue,
          updatedTimeMap
        ) -> finalQueue
      }
  }

  override def initialState(): AndThenPState[T1, T2, S1, S2] =
    AndThenPState(first.initialState(), PQueue.empty, second.initialState(), PQueue.empty, m.Queue.empty)

  @tailrec
  private def process(
    first: QI[T1],
    second: QI[T2],
    total: QI[(Idx, Idx)],
    timeMap: TimeMap
  ): (QI[T1], QI[T2], QI[(Idx, Idx)], TimeMap) = {

    def default: (QI[T1], QI[T2], QI[(Idx, Idx)], TimeMap) = (first, second, total, timeMap)

    (first.headOption, second.headOption) match {
      case (None, _) => default
      case (_, None) => default
      case (Some(IdxValue(start1, end1, value1)), Some(IdxValue(start2, end2, value2))) =>
        if (value1.isFail) {
          process(
            first.behead(),
            PQueue.unwindWhile(second)(_.end <= start1),
            total.enqueue(IdxValue(start1, end1, Fail)),
            QueueUtils.unwindWhile(timeMap)(t => t._1 < start1)
          )
        } else if (value2.isFail) {
          // Do not return Fail for the first part yet, unless it is the end of the queue
          first.size match {
            case 1 =>
              process(
                first.rewindTo(end2 + 1),
                second.behead(),
                total.enqueue(IdxValue(start1, end2, Fail)),
                QueueUtils.unwindWhile(timeMap)(t => t._1 < end2)
              )
            case _ => process(first, second.behead(), total, QueueUtils.unwindWhile(timeMap)(t => t._1 < end2))
          }
        } else { // at this moment both first and second results are not Fail.
          // late event from second, just skip it and fail this part only.
          // first            |-------|
          // second  |------|
          if (start1 > end2) {
            process(first, second.behead(), total.enqueue(IdxValue(start2, end2, Fail)))
          }
          // Gap between first and second. Just behead first and fail this part only.
          // first   |-------|
          // second             |------|
          else if (end1 + 1 < start2) {
            process(first.behead(), second, total.enqueue(IdxValue(start1, end1, Fail)))
          }
          // First and second intersect
          // first   |-------|
          // second       |-------|
          // result  |------------| (take union, not intersection)
          else {
            val end = Math.max(end1 + 1, end2)
            val start = Math.min(start1, start2)
            val newResult = IdxValue(start, end, Succ((start, end))) // todo nobody uses the output of AndThen pattern. Let's drop it later.
            process(first.rewindTo(end + 1), second.rewindTo(end + 1), total.enqueue(newResult))
          }
        }
    }
  }

  private val offset: Window = AndThenPattern.computeOffset(second)

  override val patternTag: PatternTag = AndThenPatternTag
}

case class AndThenPState[T1, T2, State1, State2](
  firstState: State1,
  firstQueue: PQueue[T1],
  secondState: State2,
  secondQueue: PQueue[T2],
  indexTimeMap: TimeMap
)

object AndThenPattern {

  type TimeMap = m.Queue[(Idx, Time)]

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def computeOffset(pattern: Pattern[_, _, _]): Window = {
    pattern.patternTag match {
      // handle special cases
      case AndThenPatternTag =>
        val (first, second) = AndThenPatternTag.getTwoInnerPatterns(pattern)
        Window(computeOffset(first).toMillis + computeOffset(second).toMillis)
      case TimerPatternTag => pattern.asInstanceOf[TimerPattern[_, _, _]].window
      case WaitPatternTag  => pattern.asInstanceOf[WaitPattern[_, _, _]].window

      // rest patterns are below
      case _: WithoutInnerPatternTag  => Window(0L)
      case tag: WithInnersPatternsTag =>
        //take the biggest offset for patterns with process patterns
        Window(tag.getInnerPatterns(pattern).map((computeOffset _).andThen(_.toMillis)).fold(0L)(Math.max))
    }

  }

}
