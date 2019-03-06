package ru.itclover.tsp.v2

import scala.language.higherKinds
import cats.{Foldable, Functor, Monad}
import cats.implicits._

import scala.collection.{mutable => m}
import ru.itclover.tsp.Segment
import ru.itclover.tsp.core.Time
import ru.itclover.tsp.v2.IdxValue.IdxValueSegment
import ru.itclover.tsp.v2.Pattern.{QI, TsIdxExtractor}

/**
  * Pattern that transforms IdxValue[T] into IdxValue[Segment(fromTime, toTime)],
  * useful when you need not a single point, but the whole time-segment as the result.
  * __Note__ - all inner patterns should be using exactly __TsIdxExtractor__, or segment bounds would be incorrect.
  *
  * @param extractor special kind of extractor for Segments creation
  * @tparam E Event
  * @tparam Inner Inner result
  * @tparam S Holds State for the next step AND results (wrong named `queue`)
  * @tparam F Container for state (some simple monad mostly)
  */
class IdxToSegmentsP[E, Inner, S <: PState[Inner, S]](inner: Pattern[E, S, Inner])(
  implicit extractor: TsIdxExtractor[E]
) extends Pattern[E, WrappingPState[Inner, S, Segment], Segment] {

  override def initialState(): WrappingPState[Inner, S, Segment] =
    WrappingPState.empty[Inner, S, Segment](inner.initialState())

  override def apply[F[_]: Monad, Cont[_]: Foldable: Functor](
    oldState: WrappingPState[Inner, S, Segment],
    events: Cont[E]
  ): F[WrappingPState[Inner, S, Segment]] = {
    val newInnerState = inner.apply[F, Cont](oldState.inner, events)
    for (innerS <- newInnerState) yield {
      val newSegments: QI[Segment] = innerS.queue.collect {
        case idxVal if idxVal.value.isSuccess =>
          val (fromTs, toTs) = (extractor.idxToTs(idxVal.start), extractor.idxToTs(idxVal.end))
          val segment = Result.succ(Segment(Time(toMillis = fromTs), Time(toMillis = toTs)))
          IdxValueSegment(idxVal.index, idxVal.start, idxVal.end, segment)
      }
      WrappingPState(innerS, newSegments)
    }
  }
}

/**
  * Simple state that wraps internal PState with `queue[T]`
  */
case class WrappingPState[Inner, S <: PState[Inner, S], T](inner: S, override val queue: QI[T])
    extends PState[T, WrappingPState[Inner, S, T]] {

  override def copyWithQueue(newQueue: QI[T]): WrappingPState[Inner, S, T] = this.copy(queue = newQueue)
}

object WrappingPState {

  def empty[Inner, S <: PState[Inner, S], T](inner: S): WrappingPState[Inner, S, T] = {
    new WrappingPState(inner, m.Queue.empty[IdxValue[T]])
  }
}
