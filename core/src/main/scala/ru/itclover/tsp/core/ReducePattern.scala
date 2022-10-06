package ru.itclover.tsp.core
import cats.instances.list._
import cats.syntax.functor._
import cats.syntax.traverse._
import cats.{Foldable, Functor, Monad}
import ru.itclover.tsp.core.Pattern.QI

import scala.annotation.tailrec

/** Reduce Pattern.
  * Complex pattern combining Seq of inner patterns (all of them have to have the same type).
  * Each inner can be transformed using `transform` function, filtered using `filterCond`.
  * Final result is left-folded of `initial` and Seq of inner results with function `func`.
  * */
class ReducePattern[Event, S, T1, T2](
  val patterns: Seq[Pattern[Event, S, T1]]
)(
  val func: (Result[T2], Result[T1]) => Result[T2],
  val transform: Result[T2] => Result[T2],
  val filterCond: Result[T1] => Boolean,
  val initial: Result[T2]
) extends Pattern[Event, ReducePState[S, T1], T2] {

  override def apply[F[_]: Monad, Cont[_]: Foldable: Functor](
    oldState: ReducePState[S, T1],
    oldQueue: PQueue[T2],
    events: Cont[Event]
  ): F[(ReducePState[S, T1], PQueue[T2])] = {
    val patternsF: List[F[(S, PQueue[T1])]] =
      patterns.zip(oldState.stateAndQueues).map { case (p, (s, q)) => p.apply[F, Cont](s, q, events) }.toList
    val patternsG: F[List[(S, PQueue[T1])]] = patternsF.traverse(identity)
    for (pG <- patternsG) yield {
      val (updatedQueues, newFinalQueue) = processQueues(pG.map(_._2), oldQueue)
      ReducePState(pG.zip(updatedQueues).map { case ((p, _), q) => p -> q }) -> newFinalQueue
    }
  }

  private def processQueues(qs: Seq[QI[T1]], resultQ: QI[T2]): (Seq[QI[T1]], QI[T2]) = {

    @tailrec
    def inner(queues: Seq[QI[T1]], result: QI[T2]): (Seq[QI[T1]], QI[T2]) = {

      def default: (Seq[QI[T1]], QI[T2]) = (queues, result)

      queues.map(_.headOption) match {
        // if any of parts is empty -> do nothing
        case x if x.contains(None) => default
        case x =>
          val ivs = x.map(_.get) // it's safe since it does not contain None
          val values = ivs.map(_.value)
          val starts = ivs.map(_.start)
          val ends = ivs.map(_.end)

          // we emit result only if results on all sides have result for same interval of indexes
          val commonStart = starts.max
          val commonEnd = ends.min
          val newQueue = queues.map(_.rewindTo(commonEnd + 1))
          val newResult =
            if (commonEnd >= commonStart) {
              val res: Result[T2] = transform(values.filter(filterCond).foldLeft(initial)(func))
              result.enqueue(IdxValue(commonStart, commonEnd, res))
            } else {
              result
            }

          inner(newQueue, newResult)
      }
    }

    inner(qs, resultQ)
  }

  override def initialState(): ReducePState[S, T1] = ReducePState(
    patterns.map(p => p.initialState() -> PQueue.empty[T1])
  )
}

case class ReducePState[State, T1](stateAndQueues: Seq[(State, PQueue[T1])])
