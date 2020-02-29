package ru.itclover.tsp.core
import cats.syntax.flatMap._
import cats.syntax.foldable._
import cats.syntax.functor._
import cats.{Apply, Foldable, Functor, Monad}
import ru.itclover.tsp.core.AndThenPattern.TimeMap
import ru.itclover.tsp.core.Pattern.IdxExtractor._
import ru.itclover.tsp.core.Pattern.{Idx, IdxExtractor, QI}
import ru.itclover.tsp.core.aggregators.{TimerPattern, WaitPattern}
import ru.itclover.tsp.core.io.TimeExtractor
import ru.itclover.tsp.core.io.TimeExtractor._

import scala.annotation.tailrec
import scala.collection.{mutable => m}
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
      event.foldLeft(oldState.indexTimeMap) { case (a, b) => a += (b.index -> b.time) }

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
    AndThenPState(first.initialState(), PQueue.empty, second.initialState(), PQueue.empty, m.TreeMap.empty)

  private def process(
    firstQ: QI[T1],
    secondQ: QI[T2],
    totalQ: QI[(Idx, Idx)],
    timeMapQ: TimeMap
  ): (QI[T1], QI[T2], QI[(Idx, Idx)], TimeMap) = {
    import Time._
    import cats.instances.option._
    import Ordered._

    // This function cleans up timeMap unwinding all elements for idx < Math.min(idx1, idx2)
    // dropWhile very expensive as it copies whole TreeMap every time.
    def cleanTimeMap(timeMap: TimeMap, idx1: Idx, idx2: Idx): TimeMap =
      timeMap.rangeImpl(Some(Math.min(idx1 - 1, idx2 - 1)), None)

    @tailrec
    def inner(
      first: QI[T1],
      second: QI[T2],
      total: QI[(Idx, Idx)],
      timeMap: TimeMap
    ): (QI[T1], QI[T2], QI[(Idx, Idx)], TimeMap) = {

      def default: (QI[T1], QI[T2], QI[(Idx, Idx)], TimeMap) = (first, second, total, timeMap)

      (first.headOption, second.headOption) match {
        case (None, _)                                                                    => default
        case (_, None)                                                                    => default
        case (Some(IdxValue(start1, end1, value1)), Some(IdxValue(start2, end2, value2))) =>
          // actually the second result starts after some offset.
          val firstStart = timeMap(start1) + offset
          // we add +1 to the end of the first result to handle queries like 'a = 0 andThen a = 1',
          // there actually intervals of 1st and 2nd successful parts don't intersect, but conceptually this query
          // should return true on margins, where 'a' changes from 0 to 1.
          val firstEnd = if (end1 >= timeMap.last._1) timeMap.last._2 else timeMap(end1 + 1) + offset

          val secondStart = timeMap(start2)
          val secondEnd = timeMap(end2)

          if (value1.isFail) {
            inner(
              first.behead(),
              second.rewindTo(QueueUtils.find(timeMap, firstStart).getOrElse(timeMap.last._1)),
              total.enqueue(IdxValue(start1, QueueUtils.find(timeMap, firstEnd).getOrElse(timeMap.last._1), Fail)),
              cleanTimeMap(timeMap, start1, start2)
            )
          } else if (value2.isFail) {
            inner(
              first.rewindTo(QueueUtils.find(timeMap, secondEnd - offset).getOrElse(timeMap.head._1)),
              second.behead(),
              total.enqueue(IdxValue(start1, end2, Fail)),
              cleanTimeMap(timeMap, start1, start2)
            )
          } else { // at this moment both first and second results are not Fail.

            // late event from second, just skip it and fail this part only.
            // first            |-------|
            // second  |------|
            if (firstStart > secondEnd) {
              inner(
                first,
                second.behead(),
                total.enqueue(IdxValue(start2, end2, Fail)),
                timeMap.rangeImpl(Some(end2), None)
              )
            }
            // Gap between first and second. Just behead first and fail this part only.
            // first   |-------|
            // second             |------|
            else if (firstEnd < secondStart) {
              inner(
                first.behead(),
                second,
                total.enqueue(IdxValue(start1, end1, Fail)),
                timeMap.rangeImpl(Some(end1), None)
              )
            }
            // First and second intersect
            // first   |-------|
            // second       |-------|
            // result       |--| (take intersection)
            else {
              val start = Math.max(QueueUtils.find(timeMap, firstStart).getOrElse(timeMap.head._1), start2)
              val end = Math.min(QueueUtils.find(timeMap, firstEnd).getOrElse(timeMap.last._1), end2)
              // todo nobody uses the output of AndThen pattern. Let's drop it later.
              val newResult = IdxValue(start, end, Succ((start, end)))
              inner(
                //todo should it be changed to QueueUtils.find(timeMap, end - offset).getOrElse(timeMap.head._1)) ???
                first.rewindTo(end + 1),
                second.rewindTo(end + 1),
                total.enqueue(newResult),
                timeMap.rangeImpl(Some(end), None)
              )
            }
          }
      }
    }

    // We use TreeMap.rangeImpl everywhere inside @inner function, which creates new view of the same RB-tree.
    // To avoid memory leaks we need to clone all remaining elements from timeMap to the new one calling .clone method.
    val toReturn = inner(firstQ, secondQ, totalQ, timeMapQ)
    toReturn.copy(_4 = toReturn._4.clone())
  }

  private val offset: Window = PatternTag.computeOffset(second)

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
  type TimeMap = m.TreeMap[Idx, Time]
}
