package ru.itclover.tsp.v2
import ru.itclover.tsp.v2.Pattern.Idx

import scala.language.implicitConversions

trait PQueue[T] {

  def size: Int
  @inline def headOption: Option[IdxValue[T]]
  @inline def dequeue(): (IdxValue[T], PQueue[T])
  @inline def dequeueOption(): Option[(IdxValue[T], PQueue[T])]
  @inline def behead(): PQueue[T]
  @inline def beheadOption(): Option[PQueue[T]]
  @inline def enqueue(idxValues: IdxValue[T]*): PQueue[T]
  @inline def enqueue(idx: Idx, value: Result[T]): PQueue[T]
  @inline def clean(): PQueue[T]
  def drop(i: Long): PQueue[T] = (1l to i).foldLeft(this) { case (x, _) => x.behead() }

  def toSeq: Seq[IdxValue[T]]
}

object PQueue {

  def apply[T](idxValue: IdxValue[T]): PQueue[T] = MutablePQueue(collection.mutable.Queue(idxValue))

  def empty[T]: PQueue[T] = MutablePQueue(collection.mutable.Queue.empty)

  case class ImmutablePQueue[T](queue: scala.collection.immutable.Queue[IdxValue[T]]) extends PQueue[T] {

    override def headOption: Option[IdxValue[T]] = queue.headOption
    override def dequeue(): (IdxValue[T], PQueue[T]) = queue.dequeue match {
      case (idx, q) => idx -> ImmutablePQueue(q)
    }
    override def dequeueOption(): Option[(IdxValue[T], PQueue[T])] = queue.dequeueOption.map {
      case (idx, q) => idx -> ImmutablePQueue(q)
    }
    override def behead(): PQueue[T] = ImmutablePQueue(queue.tail)
    override def beheadOption(): Option[PQueue[T]] = queue.dequeueOption.map(x => ImmutablePQueue(x._2))
    override def enqueue(idx: Idx, value: Result[T]): PQueue[T] = ImmutablePQueue(queue.enqueue(IdxValue(idx, value)))

    override def clean(): PQueue[T] = ImmutablePQueue(collection.immutable.Queue.empty)
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
      queue.dequeue()
      this
    }
    override def beheadOption(): Option[PQueue[T]] = if (queue.nonEmpty) { queue.dequeue(); Some(this) } else None
    override def enqueue(idx: Idx, value: Result[T]): PQueue[T] = { queue.enqueue(IdxValue(idx, value)); this }

    override def clean(): PQueue[T] = MutablePQueue(collection.mutable.Queue.empty)
    override def enqueue(idxValues: IdxValue[T]*): PQueue[T] = {
      queue.enqueue(idxValues: _*)
      this
    }
    override def toSeq: Seq[IdxValue[T]] = queue
    override def size: Int = queue.size
  }

  case class IdxMapPQueue[A, T](queue: PQueue[A], func: IdxValue[A] => Result[T]) extends PQueue[T] {
    override def size: Int = queue.size
    override def headOption: Option[IdxValue[T]] = queue.headOption.map(x => x.map(_ => func(x)))
    override def dequeue(): (IdxValue[T], PQueue[T]) = {
      val (idx, pqueue) = queue.dequeue()
      (idx.map(_ => func(idx)), IdxMapPQueue(pqueue, func))
    }
    override def dequeueOption(): Option[(IdxValue[T], PQueue[T])] =
      queue.dequeueOption().map { case (idx, pqueue) => (idx.map(_ => func(idx)), IdxMapPQueue(pqueue, func)) }
    override def behead(): PQueue[T] = IdxMapPQueue(queue.behead(), func)
    override def beheadOption(): Option[PQueue[T]] = queue.beheadOption().map(q => IdxMapPQueue(q, func))
    override def enqueue(idxValues: IdxValue[T]*): PQueue[T] = throw new UnsupportedOperationException(
      "Cannot enqueue to IdxMapPQueue! Bad logic"
    )
    override def enqueue(idx: Idx, value: Result[T]): PQueue[T] = throw new UnsupportedOperationException(
      "Cannot enqueue to IdxMapPQueue! Bad logic"
    )
    override def clean(): PQueue[T] = IdxMapPQueue(queue.clean(), func)

    override def toSeq: Seq[IdxValue[T]] = queue.toSeq.map(x => x.map(_ => func(x)))
  }

  object MapPQueue {

    def apply[A, T](queue: PQueue[A], func: A => Result[T]): IdxMapPQueue[A, T] = IdxMapPQueue(queue, {
      idx: IdxValue[A] =>
        idx.value.flatMap(func)
    })
  }
}
