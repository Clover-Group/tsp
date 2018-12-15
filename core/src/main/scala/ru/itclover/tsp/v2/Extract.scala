package ru.itclover.tsp.v2
import cats.implicits._
import cats.{Functor, Id, Monad}
import ru.itclover.tsp.core.{Time, Window}
import ru.itclover.tsp.io.TimeExtractor
import ru.itclover.tsp.io.TimeExtractor.GetTime
import ru.itclover.tsp.v2.Extract.IdxExtractor._
import ru.itclover.tsp.v2.Extract.{Idx, IdxExtractor, _}

import scala.annotation.tailrec
import scala.collection.{mutable => m}
import scala.language.higherKinds

//todo use Apache Arrow here. A lot of optimizations can be done.

object Extract {

  type Idx = Int
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

trait AddToQueue[F[_]] {
  def addToQueue[T](events: F[T], queue: m.Queue[T]): m.Queue[T]
}

object AddToQueue {

  def apply[T[_]: AddToQueue]: AddToQueue[T] = implicitly[AddToQueue[T]]

  implicit val idInstance: AddToQueue[Id] = new AddToQueue[Id] {
    override def addToQueue[T](events: Id[T], queue: m.Queue[T]): m.Queue[T] = {queue.enqueue(events); queue}
  }

  implicit val seqInstance: AddToQueue[Seq] = new AddToQueue[Seq] {
    override def addToQueue[T](events: Seq[T], queue: m.Queue[T]): m.Queue[T] = queue ++ events
  }

  implicit val listInstance: AddToQueue[List] = new AddToQueue[List] {
    override def addToQueue[T](events: List[T], queue: m.Queue[T]): m.Queue[T] = queue ++ events
  }
}

trait Pattern[Event, T, S <: PState[T, S], F[_], Cont[_]] extends ((S, Cont[Event]) => F[S]) with Serializable {

  def initialState(): S
}
