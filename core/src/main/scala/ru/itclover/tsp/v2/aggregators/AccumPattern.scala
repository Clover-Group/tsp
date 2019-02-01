package ru.itclover.tsp.v2.aggregators

import cats.implicits._
import cats.{Foldable, Functor, Monad, Order}
import ru.itclover.tsp.core.{Time, Window}
import ru.itclover.tsp.io.TimeExtractor
import ru.itclover.tsp.io.TimeExtractor.GetTime
import ru.itclover.tsp.v2.Pattern.IdxExtractor._
import ru.itclover.tsp.v2.Pattern._
import ru.itclover.tsp.v2._
import scala.annotation.tailrec
import scala.collection.{mutable => m}
import scala.language.higherKinds

trait AggregatorPatterns[Event, S <: PState[T, S], T] extends Pattern[Event, S, T]

case class AggregatorPState[InnerState, AState <: AccumState[_, Out, AState], Out](
  innerState: InnerState,
  astate: AState,
  override val queue: QI[Out],
  indexTimeMap: m.Queue[(Idx, Time)]
)(
  implicit idxOrd: Order[Idx]
) extends PState[Out, AggregatorPState[InnerState, AState, Out]] {
  override def copyWithQueue(queue: QI[Out]): AggregatorPState[InnerState, AState, Out] = this.copy(queue = queue)
}

abstract class AccumPattern[
  Event: IdxExtractor: TimeExtractor, Inner <: PState[InnerOut, Inner], InnerOut, Out,
  AState <: AccumState[InnerOut, Out, AState]
](implicit idxOrd: Order[Idx])
    extends AggregatorPatterns[Event, AggregatorPState[Inner, AState, Out],Out] {

  val innerPattern: Pattern[Event,Inner, InnerOut]
  val window: Window

  override def apply[F[_]: Monad, Cont[_]: Foldable: Functor](
    state: AggregatorPState[Inner, AState, Out],
    event: Cont[Event]
  ): F[AggregatorPState[Inner, AState, Out]] = {

    val idxTimeMapWithNewEvents =
      event.map(e => e.index -> e.time).foldLeft(state.indexTimeMap) { case (a, b) => { a.enqueue(b); a } }

    innerPattern
      .apply[F, Cont](state.innerState, event)
      .map(
        newInnerState => {
          val (newInnerQueue, newAState, newResults, updatedIndexTimeMap) =
            processQueue(newInnerState, state.astate, idxTimeMapWithNewEvents)

          AggregatorPState(
            newInnerState.copyWithQueue(newInnerQueue),
            newAState, { state.queue.enqueue(newResults: _*); state.queue },
            updatedIndexTimeMap
          )(idxOrd)
        }
      )
  }

  private def processQueue(
    innerS: Inner,
    accumState: AState,
    indexTimeMap: m.Queue[(Idx, Time)]
  ): (QI[InnerOut], AState, QI[Out], m.Queue[(Idx, Time)]) = {

    @tailrec
    def innerFunc(
      innerQueue: QI[InnerOut],
      accumState: AState,
      collectedNewResults: QI[Out],
      indexTimeMap: m.Queue[(Idx, Time)]
    ): (QI[InnerOut], AState, QI[Out], m.Queue[(Idx, Time)]) =
      innerQueue.headOption match {
        case None => (innerQueue, accumState, collectedNewResults, indexTimeMap)
        case Some(IdxValue(index, value)) =>
          val updatedQueue = { innerQueue.dequeue(); innerQueue }
          val (newInnerResultTime, updatedIdxTimeMap) = QueueUtils.rollMap(index, indexTimeMap)(idxOrd)

          val (newAState, newResults) = accumState.updated(window, index, newInnerResultTime, value)

          innerFunc(
            updatedQueue,
            newAState,
            { collectedNewResults.enqueue(newResults: _*); collectedNewResults },
            updatedIdxTimeMap
          )
      }

    innerFunc(innerS.queue, accumState, m.Queue.empty, indexTimeMap)
  }

}

trait AccumState[In, Out, +Self <: AccumState[In, Out, Self]] extends Product with Serializable {

  def updated(window: Window, idx: Idx, time: Time, value: Result[In]): (Self, QI[Out])
}
