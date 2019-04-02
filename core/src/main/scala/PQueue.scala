package ru.itclover.tsp.v2
import ru.itclover.tsp.v2.Pattern.Idx

import scala.collection.immutable
import scala.language.implicitConversions

trait PQueue[T] {

  def size: Int
  def headOption: Option[IdxValue[T]]
  def dequeue(): (IdxValue[T], PQueue[T])
  def dequeueOption(): Option[(IdxValue[T], PQueue[T])]
  def behead(): PQueue[T]
  def beheadOption(): Option[PQueue[T]]
  def enqueue(idxValue: IdxValue[T]): PQueue[T]
  def enqueue(idxValues: IdxValue[T]*): PQueue[T]
  def enqueue(idx: Idx, value: Result[T]): PQueue[T]
  def clean(): PQueue[T]
  def map[B](f: IdxValue[T] => IdxValue[B]): PQueue[B]

  def drop(i: Long): PQueue[T] = (1l to i).foldLeft(this) { case (x, _) => x.behead() }

  def toSeq: Seq[IdxValue[T]]
}

object PQueue {

  def empty[T]: PQueue[T] = MutablePQueue(collection.mutable.Queue.empty)

  case class ImmutablePQueue[T](queue: scala.collection.immutable.Queue[IdxValue[T]]) extends PQueue[T]{

    override def headOption: Option[IdxValue[T]] = queue.headOption
    override def dequeue(): (IdxValue[T], PQueue[T]) = queue.dequeue match {
      case (idx, q) => idx -> ImmutablePQueue(q)
    }
    override def dequeueOption(): Option[(IdxValue[T], PQueue[T])] = queue.dequeueOption.map {
      case (idx, q) => idx -> ImmutablePQueue(q)
    }
    override def behead(): PQueue[T] = ImmutablePQueue(queue.tail)
    override def beheadOption(): Option[PQueue[T]] = queue.dequeueOption.map(x => ImmutablePQueue(x._2))
    override def enqueue(idxValue: IdxValue[T]): PQueue[T] = ImmutablePQueue(queue.enqueue(idxValue))
    override def enqueue(idx: Idx, value: Result[T]): PQueue[T] = ImmutablePQueue(queue.enqueue(IdxValue(idx, value)))

    override def clean(): PQueue[T] = ImmutablePQueue(collection.immutable.Queue.empty)
    override def map[B](f: IdxValue[T] => IdxValue[B]): PQueue[B] = ImmutablePQueue(queue.map(f))
    override def enqueue(
      idxValues: IdxValue[T]*
    ): PQueue[T] = {
      ImmutablePQueue(queue.enqueue(scala.collection.immutable.Iterable(idxValues: _*)))
    }
    override def toSeq: Seq[IdxValue[T]] = queue.toSeq
    override def size: Int = queue.size
  }

  case class MutablePQueue[T](queue: scala.collection.mutable.Queue[IdxValue[T]]) extends PQueue[T] {

    override def headOption: Option[IdxValue[T]] = queue.headOption
    override def dequeue(): (IdxValue[T], PQueue[T]) = {
      val result = queue.dequeue
      result -> this
    }
    override def dequeueOption(): Option[(IdxValue[T], PQueue[T])] = {
      if (queue.nonEmpty) {
        Some(queue.dequeue -> this)
      } else None

    }
    override def behead(): PQueue[T] = {
      queue.tail
      this
    }
    override def beheadOption(): Option[PQueue[T]] = if (queue.nonEmpty) { queue.dequeue(); Some(this) } else None
    override def enqueue(idxValue: IdxValue[T]): PQueue[T] = { queue.enqueue(idxValue); this }
    override def enqueue(idx: Idx, value: Result[T]): PQueue[T] = { queue.enqueue(IdxValue(idx, value)); this }

    override def clean(): PQueue[T] = MutablePQueue(collection.mutable.Queue.empty)
    override def map[B](f: IdxValue[T] => IdxValue[B]): PQueue[B] = MutablePQueue(queue.map(f))
    override def enqueue(idxValues: IdxValue[T]*): PQueue[T] = {
      queue.enqueue(idxValues: _*)
      this
    }
    override def toSeq: Seq[IdxValue[T]] = queue.toSeq
    override def size: Int = queue.size
  }

}
