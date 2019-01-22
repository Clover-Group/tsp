package ru.itclover.tsp.v2
import cats.Monad
import cats.implicits._
import ru.itclover.tsp.v2.Pattern.QI

import scala.annotation.tailrec
import scala.collection.{mutable => m}
import scala.language.higherKinds

/** Reduce Pattern */

class ReducePattern[Event, S <: PState[T1, S], T1, T2, F[_] : Monad, Cont[_]](
  patterns: Seq[Pattern[Event, T1, S, F, Cont]]
)(
  func: (Result[T2], Result[T1]) => Result[T2],
  filterCond: Result[T1] => Boolean,
  initial: Result[T2]
) extends Pattern[Event, T2, ReducePState[S, T1, T2], F, Cont] {

  override def apply(
    oldState: ReducePState[S, T1, T2],
    events: Cont[Event]
  ): F[ReducePState[S, T1, T2]] = {
    //val leftF = left.apply(oldState.left, events)
    //val rightF = right.apply(oldState.right, events)
    val patternsF: List[F[S]] = patterns.zip(oldState.states).map { case (p, s) => p.apply(s, events) }.toList
    val patternsG: F[List[S]] = patternsF.traverse(identity)
    for (pG <- patternsG) yield {
      val (updatedQueues, newFinalQueue) =
        processQueues(pG.map(_.queue), oldState.queue)
      ReducePState(
        pG.zip(updatedQueues).map { case (p, q) => p.copyWithQueue(q) },
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
          if (indices.forall(_ == ivs.head.index)) {
            val res: Result[T2] = values.filter(filterCond).foldLeft(initial)(func)
            inner(queues.map(q => { q.dequeue; q }), { result.enqueue(IdxValue(ivs.head.index, res)); result })
            // otherwise skip results from one of sides (with minimum index)
          } else {
            val idxOfMinIndex = indices.zipWithIndex.minBy(_._1)._2
            inner(queues.zipWithIndex.map { case (q, i) => if (i == idxOfMinIndex) { q.dequeue; q } else q }, result)
          }
      }
    }

    inner(qs, resultQ)
  }

  override def initialState(): ReducePState[S, T1, T2] =
    ReducePState(patterns.map(_.initialState()), m.Queue.empty)
}

case class ReducePState[State <: PState[T1, State], T1, T2](
  states: Seq[State],
  override val queue: QI[T2]
) extends PState[T2, ReducePState[State, T1, T2]] {
  override def copyWithQueue(queue: QI[T2]): ReducePState[State, T1, T2] = this.copy(queue = queue)
}

case object ReducePState {}
