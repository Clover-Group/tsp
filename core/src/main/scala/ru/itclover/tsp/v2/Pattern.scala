package ru.itclover.tsp.v2
import ru.itclover.tsp.v2.Pattern.{Idx, _}

import scala.collection.{mutable => m}
import scala.language.higherKinds

//todo use Apache Arrow here. A lot of optimizations can be done.

trait Pattern[Event, T, S <: PState[T, S], F[_], Cont[_]] extends ((S, Cont[Event]) => F[S]) with Serializable {

  def initialState(): S
}

trait PState[T, +Self <: PState[T, _]] {
  def queue: QI[T]
  def copyWithQueue(queue: QI[T]): Self
}

case class IdxValue[T](index: Idx, value: Result[T])

object Pattern {

  type Idx = Long
  type QI[T] = m.Queue[IdxValue[T]]

  trait IdxExtractor[Event] extends Serializable {
    def apply(e: Event): Idx
  }

  object IdxExtractor {
    implicit class GetIdx[T](val event: T) extends AnyVal {
      def index(implicit te: IdxExtractor[T]): Idx = te.apply(event)
    }

    def of[E](f: E => Idx): IdxExtractor[E] = new IdxExtractor[E] {
      override def apply(e: E): Idx = f(e)
    }
  }
}
