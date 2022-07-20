package ru.itclover.tsp.core
import cats.kernel.Order
import ru.itclover.tsp.core.Pattern.Idx

import scala.annotation.tailrec
import scala.collection.{mutable => m}

object QueueUtils {

  private val trueFunction = (_: Any) => true

  def takeWhileFromQueue[A](queue: m.ArrayDeque[A])(predicate: A => Boolean = trueFunction): (m.ArrayDeque[A], m.ArrayDeque[A]) =
    if (predicate.eq(trueFunction)) (queue, m.ArrayDeque.empty)
    else {

      @tailrec
      def inner(result: m.ArrayDeque[A], q: m.ArrayDeque[A]): (m.ArrayDeque[A], m.ArrayDeque[A]) =
        q.headOption match {
          case Some(x) if predicate(x) => inner({ result.append(x); result }, { q.removeHead(); q })
          case _                       => (result, q)
        }

      inner(m.ArrayDeque.empty, queue)
    }

  /**
    * Splits inner `q` at point idx, so all records with id < idx are in first returned queue, and all with id >= idx are in second.
    */
  def splitAtIdx(q: m.ArrayDeque[(Idx, Time)], idx: Idx, marginToFirst: Boolean = false)(
    implicit ord: Order[Idx]
  ): (m.ArrayDeque[(Idx, Time)], m.ArrayDeque[(Idx, Time)]) = {
    takeWhileFromQueue(q) {
      if (marginToFirst) {
        case (idx1: Idx, _: Time) => ord.lteqv(idx1, idx)
      } else {
        case (idx1: Idx, _: Time) => ord.lt(idx1, idx)
      }
    }
  }

}
