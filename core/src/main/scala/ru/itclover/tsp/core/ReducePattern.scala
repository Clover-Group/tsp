package ru.itclover.tsp.core
import cats.implicits._
import cats.{Foldable, Functor, Monad, Order}
import ru.itclover.tsp.core.Pattern.{Idx, QI}

import scala.annotation.tailrec
import scala.language.higherKinds

/** Reduce Pattern */

class ReducePattern[Event, S <: PState[T1, S], T1, T2](
  patterns: Seq[Pattern[Event, S, T1]]
)(
  func: (Result[T2], Result[T1]) => Result[T2],
  transform: Result[T2] => Result[T2],
  filterCond: Result[T1] => Boolean,
  initial: Result[T2]
)(
  implicit idxOrd: Order[Idx]
) extends Pattern[Event, ReducePState[S, T1, T2], T2] {

  override def apply[F[_]: Monad, Cont[_]: Foldable: Functor](
    oldState: ReducePState[S, T1, T2],
    events: Cont[Event]
  ): F[ReducePState[S, T1, T2]] = {
    //val leftF = left.apply(oldState.left, events)
    //val rightF = right.apply(oldState.right, events)
    val patternsF: List[F[S]] = patterns.zip(oldState.states).map { case (p, s) => p.apply[F, Cont](s, events) }.toList
    val patternsG: F[List[S]] = patternsF.traverse(identity)
    for (pG <- patternsG) yield {
      val (updatedQueues, newFinalQueue) =
        processQueues(pG.map(_.queue), oldState.queue)
      ReducePState(
        pG.zip(updatedQueues).map { case (p, q) => p.copyWith(q) },
        newFinalQueue
      )
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
          val indices = ivs.map(_.index)
          // we emit result only if results on all sides come at the same time
          if (indices.forall(i => idxOrd.eqv(i, ivs.head.index))) {
            val res: Result[T2] = transform(values.filter(filterCond).foldLeft(initial)(func))
            inner(queues.map(q => q.behead()), result.enqueue(IdxValue(ivs.head.index, res)))
            // otherwise skip results from one of sides (with minimum index)
          } else {
            val idxOfMinIndex = indices.zipWithIndex.minBy(_._1)._2
            inner(queues.zipWithIndex.map { case (q, i) => if (i == idxOfMinIndex) q.behead() else q }, result)
          }
      }
    }

    inner(qs, resultQ)
  }

  override def initialState(): ReducePState[S, T1, T2] =
    ReducePState(patterns.map(_.initialState()), PQueue.empty)
}

case class ReducePState[State <: PState[T1, State], T1, T2](
  states: Seq[State],
  override val queue: QI[T2]
) extends PState[T2, ReducePState[State, T1, T2]] {
  override def copyWith(queue: QI[T2]): ReducePState[State, T1, T2] = this.copy(queue = queue)
}

case object ReducePState {}
