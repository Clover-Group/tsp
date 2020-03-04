package ru.itclover.tsp.core
import cats.kernel.Order
import ru.itclover.tsp.core.AndThenPattern.TimeMap
import ru.itclover.tsp.core.Pattern.Idx

import scala.annotation.tailrec
import scala.collection.{mutable => m}

object QueueUtils {

  private val trueFunction = (_: Any) => true

  def takeWhileFromQueue[A](queue: m.Queue[A])(predicate: A => Boolean = trueFunction): (m.Queue[A], m.Queue[A]) =
    if (predicate.eq(trueFunction)) (queue, m.Queue.empty)
    else {

      @tailrec
      def inner(result: m.Queue[A], q: m.Queue[A]): (m.Queue[A], m.Queue[A]) =
        q.headOption match {
          case Some(x) if predicate(x) => inner({ result.enqueue(x); result }, { q.dequeue(); q })
          case _                       => (result, q)
        }

      inner(m.Queue.empty, queue)
    }

  /**
    * Splits inner `q` at point idx, so all records with id < idx are in first returned queue, and all with id >= idx are in second.
    */
  def splitAtIdx(q: m.Queue[(Idx, Time)], idx: Idx, marginToFirst: Boolean = false)(
    implicit ord: Order[Idx]
  ): (m.Queue[(Idx, Time)], m.Queue[(Idx, Time)]) = {
    takeWhileFromQueue(q) {
      if (marginToFirst) {
        case (idx1: Idx, _: Time) => ord.lteqv(idx1, idx)
      } else {
        case (idx1: Idx, _: Time) => ord.lt(idx1, idx)
      }
    }
  }

  /** Removes elements from m.Queue until predicate is true */
  @scala.annotation.tailrec
  def unwindWhile[T](queue: m.Queue[T])(func: T => Boolean): m.Queue[T] = {
    queue.headOption match {
      case Some(x) if func(x) => unwindWhile({ queue.dequeue(); queue })(func)
      case _                  => queue
    }
  }

  // returns the first Idx in timeMap corresponding to the time after the given one.
  def find(timeMap: TimeMap, time: Time): Option[Idx] = {
    import Time._
    import Ordered._

    if (timeMap.isEmpty) None
    else if (timeMap.last._2 < time) None
    else if (timeMap.head._2 >= time) Some(timeMap.head._1)
    else {
      val iterator = timeMap.iterator.dropWhile({ case (i, t) => t < time })
      if (iterator.hasNext) Some(iterator.next()._1) else None
    }
  }

}
