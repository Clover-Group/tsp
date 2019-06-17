package ru.itclover.tsp.core.aggregators

import cats.implicits._
import cats.{Foldable, Functor, Monad, Order}
import ru.itclover.tsp.core.{Time, Window}
import ru.itclover.tsp.core.Pattern.IdxExtractor._
import ru.itclover.tsp.core.Pattern._
import ru.itclover.tsp.core._
import ru.itclover.tsp.core.io.TimeExtractor
import ru.itclover.tsp.core.io.TimeExtractor.GetTime

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
  override def copyWith(queue: QI[Out]): AggregatorPState[InnerState, AState, Out] = this.copy(queue = queue)
}

abstract class AccumPattern[
  Event: IdxExtractor: TimeExtractor,
  Inner <: PState[InnerOut, Inner],
  InnerOut,
  Out,
  AState <: AccumState[InnerOut, Out, AState]
](implicit idxOrd: Order[Idx])
    extends AggregatorPatterns[Event, AggregatorPState[Inner, AState, Out], Out]
    with WithInners[Event] {

  val window: Window

  def inner: Pattern[Event, Inner, InnerOut]

  override def innerPatterns: Seq[Pattern[Event, _, _]] = Seq(inner)

  override def apply[F[_]: Monad, Cont[_]: Foldable: Functor](
    state: AggregatorPState[Inner, AState, Out],
    event: Cont[Event]
  ): F[AggregatorPState[Inner, AState, Out]] = {

    val idxTimeMapWithNewEvents =
      event.foldLeft(state.indexTimeMap) { case (a, b) => a.enqueue(b.index -> b.time); a }

    inner
      .apply[F, Cont](state.innerState, event)
      .map(
        newInnerState => {
          val (newInnerQueue, newAState, newResults, updatedIndexTimeMap) =
            processQueue(newInnerState, state.astate, state.queue, idxTimeMapWithNewEvents)

          AggregatorPState(
            newInnerState.copyWith(newInnerQueue),
            newAState,
            newResults,
            updatedIndexTimeMap
          )(idxOrd)
        }
      )
  }

  private def processQueue(
    innerS: Inner,
    accumState: AState,
    results: QI[Out],
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
          val updatedQueue = innerQueue.behead()
          val (newInnerResultTime, updatedIdxTimeMap) = QueueUtils.rollMap(index, indexTimeMap)(idxOrd)

          val (newAState, newResults) = accumState.updated(window, index, newInnerResultTime, value)

          innerFunc(
            updatedQueue,
            newAState,
            collectedNewResults.enqueue(newResults.toSeq: _*),
            updatedIdxTimeMap
          )
      }

    innerFunc(innerS.queue, accumState, results, indexTimeMap)
  }

}

trait AccumState[In, Out, +Self <: AccumState[In, Out, Self]] extends Product with Serializable {

  def updated(window: Window, idx: Idx, time: Time, value: Result[In]): (Self, QI[Out])
}
