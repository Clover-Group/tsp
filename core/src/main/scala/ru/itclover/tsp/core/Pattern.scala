package ru.itclover.tsp.core

import cats.{Foldable, Functor, Monad, Order}
import org.openjdk.jmh.annotations.{Scope, State}
import ru.itclover.tsp.core.Pattern.Idx

import scala.language.higherKinds

trait Pat[Event, +T]

object Pat {

  def unapply[E, _, T](arg: Pat[E, T]): Option[Pattern[E, _, T]] = arg match {
    case x: Pattern[E, _, T] => Some(x)
  }
}

/**
  * Main trait for all patterns, basically just a function from state and events chunk to new a state.
  *
  * @tparam Event underlying data
  * @tparam T Type of the results in the S
  * @tparam S Holds State for the next step AND results (wrong named `queue`)
  */
@State(Scope.Benchmark)
trait Pattern[Event, S <: PState[T, S], T] extends Pat[Event, T] with Serializable {

  /** @return initial state. Has to be called only once */
  def initialState(): S

  /**
    * @param oldState - previous state
    * @param events - new events to be processed
    * @tparam F Container for state (some simple monad mostly)
    * @tparam Cont Container for yet another chunk of Events
    * @return new state wrapped in F[_]
    */
  @inline def apply[F[_]: Monad, Cont[_]: Foldable: Functor](oldState: S, events: Cont[Event]): F[S]
}

case class IdxValue[+T](start: Idx, end: Idx, value: Result[T]) {
  def map[B](f: T => Result[B]): IdxValue[B] = this.copy(value = value.flatMap(f))

  //todo tests
  def removeBefore(idx: Idx)(implicit idxOrd: Order[Idx]): Option[IdxValue[T]] = {
    if (idxOrd.lt(idx, start)) {
      Some(this)
    } else if (idxOrd.gt(idx, end)) {
      None
    } else {
      Some(this.copy(start = idx))
    }
  }

  def removeAfter(idx: Idx)(implicit idxOrd: Order[Idx]): Option[IdxValue[T]] = {
    if (idxOrd.lt(idx, start)) {
      None
    } else if (idxOrd.gt(idx, end)) {
      Some(this)
    } else {
      Some(this.copy(end = idx))
    }
  }
}

object IdxValue {

  /// Union the segments with a custom result
  // todo re-consider this???
  def union[T1, T2, T3](iv1: IdxValue[T1], iv2: IdxValue[T2])(
    func: (Result[T1], Result[T2]) => Result[T3]
  ): IdxValue[T3] = IdxValue(
    start = Math.min(iv1.start, iv2.start),
    end = Math.max(iv1.end, iv2.end),
    value = func(iv1.value, iv2.value)
  )
}

object Pattern {

  type Idx = Long

  type QI[T] = PQueue[T]

  trait IdxExtractor[Event] extends Serializable with Order[Idx] {
    def apply(e: Event): Idx
  }

  //todo выкинуть, переписать
  class TsIdxExtractor[Event](eventToTs: Event => Long) extends IdxExtractor[Event] {
    val maxCounter: Int = 10e5.toInt // should be power of 10
    var counter: Int = 0

    override def apply(e: Event): Idx = {
      counter = (counter + 1) % maxCounter
      tsToIdx(eventToTs(e))
    }

    override def compare(x: Idx, y: Idx): Int = idxToTs(x) compare idxToTs(y)

    def idxToTs(idx: Idx): Long = idx / maxCounter

    def tsToIdx(ts: Long): Idx = ts * maxCounter + counter //todo ts << 5 & counter ?
  }

  object IdxExtractor {
    implicit class GetIdx[T](val event: T) extends AnyVal {
      def index(implicit te: IdxExtractor[T]): Idx = te.apply(event)
    }

    def of[E](f: E => Idx): IdxExtractor[E] = new IdxExtractor[E] {
      override def apply(e: E): Idx = f(e)

      override def compare(x: Idx, y: Idx): Int = x compare y
    }
  }
}
