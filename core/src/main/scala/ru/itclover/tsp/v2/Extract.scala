package ru.itclover.tsp.v2
import cats.Id
import ru.itclover.tsp.v2.Extract.{Idx, _}

import scala.collection.{mutable => m}
import scala.language.higherKinds

//todo use Apache Arrow here. A lot of optimizations can be done.

object Extract {

  type Idx = Long
  type QI[T] = m.Queue[IdxValue[T]]

  type Result[T] = Option[T]

  object Result {
    def fail[T]: Result[T] = Fail
    def succ[T](t: T): Result[T] = Succ(t)
  }
  type Fail = None.type
  val Fail: Result[Nothing] = None
  type Succ[+T] = Some[T]

  object Succ {
    def apply[T](t: T): Succ[T] = Some(t)
    def unapply[T](arg: Succ[T]): Option[T] = Some.unapply(arg)
  }

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

case class IdxValue[+T](index: Idx, value: Result[T])

trait PState[T, +Self <: PState[T, _]] {
  def queue: QI[T]
  def copyWithQueue(queue: QI[T]): Self
}

trait Pattern[Event, T, S <: PState[T, S], F[_], Cont[_]] extends ((S, Cont[Event]) => F[S]) with Serializable {

  def initialState(): S
}
