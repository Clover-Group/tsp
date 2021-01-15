package ru.itclover.tsp.core

import cats.{Foldable, Functor, Monad, Order}
import com.typesafe.scalalogging.Logger
import ru.itclover.tsp.core.Pattern.Idx

import scala.language.higherKinds

trait Pat[Event, +T]

object Pat {
  val logger = Logger("Pat")

  def unapply[E, _, T](arg: Pat[E, T]): Option[Pattern[E, _, T]] = arg match {
    case x: Pattern[E, _, T] => Some(x)
    case _                   =>
      sys.error(s"$arg is not a pattern")
  }
}

/**
  * Main trait for all patterns, basically just a function from state and events chunk to new a state.
  *
  * @tparam Event underlying data
  * @tparam T Type of the results in the S
  * @tparam S Holds State for the next step AND results (wrong named `queue`)
  */
trait Pattern[Event, S, T] extends Pat[Event, T] with Serializable {

  /** @return initial state. Has to be called only once */
  def initialState(): S

  /**
    * @param oldState - previous state
    * @param events - new events to be processed
    * @tparam F Container for state (some simple monad mostly)
    * @tparam Cont Container for yet another chunk of Events
    * @return new state wrapped in F[_]
    */
  @inline def apply[F[_]: Monad, Cont[_]: Foldable: Functor](
    oldState: S,
    queue: PQueue[T],
    events: Cont[Event]
  ): F[(S, PQueue[T])]
}

case class IdxValue[+T](start: Idx, end: Idx, value: Result[T]) {
  def map[B](f: T => Result[B]): IdxValue[B] = this.copy(value = value.flatMap(f))

  def intersects[A >: T](that: IdxValue[A])(implicit ord: Order[Idx]): Boolean =
    !(ord.lt(this.end, that.start) || ord.gt(this.start, that.end))
}

object Pattern {

  type Idx = Long

  type QI[T] = PQueue[T]

  trait IdxExtractor[Event] extends Serializable with Order[Idx] {
    def apply(e: Event): Idx

    def comap[A](f: A => Event): IdxExtractor[A] = IdxExtractor.of(f.andThen(apply))
  }

  object IdxExtractor {
    implicit class GetIdx[T](private val event: T) extends AnyVal {
      def index(implicit te: IdxExtractor[T]): Idx = te.apply(event)
    }

    def of[E](f: E => Idx): IdxExtractor[E] = new IdxExtractor[E] {
      override def apply(e: E): Idx = f(e)

      override def compare(x: Idx, y: Idx): Int = x compare y
    }
  }
}
