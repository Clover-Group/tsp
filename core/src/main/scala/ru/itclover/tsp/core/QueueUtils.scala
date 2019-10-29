package ru.itclover.tsp.core
import cats.kernel.Order
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

}
