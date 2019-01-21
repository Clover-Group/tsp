package ru.itclover.tsp.v2

import scala.language.higherKinds
import cats.{Foldable, Monad}
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
  * @param extractor special kind of extractor for Segments creation
  * @tparam E Event
  * @tparam Inner Inner result
  * @tparam S Holds State for the next step AND results (wrong named `queue`)
  * @tparam F Container for state (some simple monad mostly)
  */
class IdxToSegmentsP[E, Inner, S <: PState[Inner, S], F[_]: Monad](
  inner: Pattern[E, Inner, S, F, Vector]
)(
  implicit extractor: TsIdxExtractor[E]
) extends Pattern[E, Segment, WrappingPState[Inner, S, Segment], F, Vector] {

  override def initialState() = WrappingPState.empty[Inner, S, Segment](inner.initialState())

  override def apply(oldState: WrappingPState[Inner, S, Segment], events: Vector[E]) = {
    val newInnerState = inner.apply(oldState.inner, events)
    for (innerS <- newInnerState) yield {
      val newSegments: QI[Segment] = innerS.queue.map { idxVal =>
        val (fromTs, toTs) = (extractor.idxToTs(idxVal.start), extractor.idxToTs(idxVal.end))
        val segment = Result.succ(Segment(Time(toMillis = fromTs), Time(toMillis = toTs)))
        IdxValueSegment(idxVal.index, idxVal.start, idxVal.end, segment)
      }
      new WrappingPState(innerS, newSegments)
    }
  }
}

/**
  * Simple state that wraps internal PState with `queue[T]`
  */
class WrappingPState[Inner, InnerS <: PState[Inner, InnerS], T](
  val inner: InnerS,
  override val queue: QI[T]
) extends PState[T, WrappingPState[Inner, InnerS, T]] {

  override def copyWithQueue(newQueue: QI[T]) = {
    new WrappingPState(inner, newQueue)
  }
}

object WrappingPState {
  def empty[Inner, InnerS <: PState[Inner, InnerS], T](inner: InnerS): WrappingPState[Inner, InnerS, T] = {
    new WrappingPState(inner, m.Queue.empty[IdxValue[T]])
  }
}
