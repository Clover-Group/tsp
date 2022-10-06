package ru.itclover.tsp.core.aggregators

import cats.syntax.foldable._
import cats.syntax.functor._
import cats.{Foldable, Functor, Monad}
import ru.itclover.tsp.core.Pattern.IdxExtractor._
import ru.itclover.tsp.core.Pattern._
import ru.itclover.tsp.core.io.TimeExtractor
import ru.itclover.tsp.core.io.TimeExtractor.GetTime
import ru.itclover.tsp.core.{Time, Window, _}

import scala.annotation.tailrec
import scala.collection.{mutable => m}

trait AggregatorPatterns[Event, S, T] extends Pattern[Event, S, T]

case class AggregatorPState[InnerState, InnerOut, AState](
  innerState: InnerState,
  innerQueue: PQueue[InnerOut],
  astate: AState,
  indexTimeMap: m.ArrayDeque[(Idx, Time)]
)

abstract class AccumPattern[Event: IdxExtractor: TimeExtractor, InnerState, InnerOut, Out, AState <: AccumState[
  InnerOut,
  Out,
  AState
]] extends AggregatorPatterns[Event, AggregatorPState[InnerState, InnerOut, AState], Out] {

  val window: Window

  def inner: Pattern[Event, InnerState, InnerOut]

  override def apply[F[_]: Monad, Cont[_]: Foldable: Functor](
    state: AggregatorPState[InnerState, InnerOut, AState],
    queue: PQueue[Out],
    event: Cont[Event]
  ): F[(AggregatorPState[InnerState, InnerOut, AState], PQueue[Out])] = {

    val idxTimeMapWithNewEvents =
      event.foldLeft(state.indexTimeMap) { case (a, b) => a.append(b.index -> b.time); a }

    inner
      .apply[F, Cont](state.innerState, state.innerQueue, event)
      .map {
        case (newInnerState, newInnerQueue) => {
          val (updatedInnerQueue, newAState, newResults, updatedIndexTimeMap) =
            processQueue(newInnerQueue, state.astate, queue, idxTimeMapWithNewEvents)

          AggregatorPState(
            newInnerState,
            updatedInnerQueue,
            newAState,
            updatedIndexTimeMap
          ) -> newResults
        }
      }
  }

  @tailrec
  private def processQueue(
    innerQueue: QI[InnerOut],
    accumState: AState,
    results: QI[Out],
    indexTimeMap: m.ArrayDeque[(Idx, Time)]
  ): (QI[InnerOut], AState, QI[Out], m.ArrayDeque[(Idx, Time)]) = {
    innerQueue.dequeueOption() match {
      case None                                               => (innerQueue, accumState, results, indexTimeMap)
      case Some((iv @ IdxValue(start, end, _), updatedQueue)) =>
        // rewind all old records
        val (_, rewinded) = QueueUtils.splitAtIdx(indexTimeMap, start)

        //idxTimeMapForValue contains info about Idx->Time for all events in range [start, end]
        val (idxTimeMapForValue, updatedIdxTimeMap) = QueueUtils.splitAtIdx(rewinded, end, marginToFirst = true)

        val (newAState, newResults) = accumState.updated(window, idxTimeMapForValue, iv)

        processQueue(updatedQueue, newAState, PQueue.spillQueueToAnother(newResults, results), updatedIdxTimeMap)
    }
  }

}

trait AccumState[In, Out, Self <: AccumState[In, Out, Self]] extends Product with Serializable {

  /** This method is called for each IdxValue produced by inner patterns.
    * @param window - defines time window for accumulation.
    * @param times - contains mapping Idx->Time for all events with Idx in [idxValue.start, idxValue.end].
    *              Guaranteed to be non-empty.
    * @param idxValue - result from inner pattern.
    * @return Tuple of updated state and queue of results to be emitted from this pattern.
    */
  def updated(window: Window, times: m.ArrayDeque[(Idx, Time)], idxValue: IdxValue[In]): (Self, QI[Out])
}
