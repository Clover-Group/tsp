package ru.itclover.tsp.core.aggregators

import ru.itclover.tsp.core.Pattern.{Idx, IdxExtractor, QI}
import ru.itclover.tsp.core.Time.MaxWindow
import ru.itclover.tsp.core.io.TimeExtractor
import ru.itclover.tsp.core._

import scala.collection.{mutable => m}

class TimestampsAdderPattern[Event: IdxExtractor: TimeExtractor, S, T](
  override val inner: Pattern[Event, S, T]
) extends AccumPattern[Event, S, T, Segment, TimestampAdderAccumState[T]] {
  override def initialState(): AggregatorPState[S, T, TimestampAdderAccumState[T]] = AggregatorPState(
    inner.initialState(),
    innerQueue = PQueue.empty,
    astate = TimestampAdderAccumState[T](),
    indexTimeMap = m.Queue.empty
  )

  override val window: Window = MaxWindow

  override def toString: String = s"TimestampsAdderPattern($inner)"
}

protected case class TimestampAdderAccumState[T]() extends AccumState[T, Segment, TimestampAdderAccumState[T]] {

  @inline
  override def updated(
    window: Window,
    times: m.Queue[(Idx, Time)],
    idxValue: IdxValue[T]
  ): (TimestampAdderAccumState[T], QI[Segment]) = {
    if (times.isEmpty) (this, PQueue.empty)
    else {
      val result = idxValue.map(_ => Succ(Segment(times.head._2, times.last._2)))
      (TimestampAdderAccumState(), PQueue.apply(result))
    }
  }
}
