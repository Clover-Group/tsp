package ru.itclover.tsp.v2.aggregators

import cats.{Foldable, Functor, Monad}
import ru.itclover.tsp.core.Time._
import ru.itclover.tsp.core.{Time, Window}
import ru.itclover.tsp.io.TimeExtractor
import ru.itclover.tsp.v2.Extract._
import ru.itclover.tsp.v2._

import scala.Ordering.Implicits._
import scala.collection.{mutable => m}
import scala.language.higherKinds

case class PreviousValue[Event: IdxExtractor: TimeExtractor, State <: PState[Out, State], Out, F[_]: Monad, Cont[_]: Functor: Foldable](
  override val innerPattern: Pattern[Event, Out, State, F, Cont],
  override val window: Window
) extends AccumPattern[Event, State, Out, Out, PreviousValueAccumState[Out], F, Cont] {
  override def initialState(): AggregatorPState[State, PreviousValueAccumState[Out], Out] =
    AggregatorPState(innerPattern.initialState(), PreviousValueAccumState(None), m.Queue.empty, m.Queue.empty)
}

case class PreviousValueAccumState[T](lastTimeValue: Option[(Time, T)])
    extends AccumState[T, T, PreviousValueAccumState[T]] {
  override def updated(window: Window, idx: Idx, time: Time, value: Result[T]): (PreviousValueAccumState[T], QI[T]) = {
    value match {
      case Fail => this -> m.Queue(IdxValue(idx, Result.fail))
      case Succ(newValue) =>
        PreviousValueAccumState(Some(time -> newValue)) ->
        this.lastTimeValue
          .filter(_._1.plus(window) >= time)
          .map(ab => m.Queue(IdxValue(idx, Result.succ(ab._2))))
          .getOrElse(m.Queue.empty)
    }
  }
}
