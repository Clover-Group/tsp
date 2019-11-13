package ru.itclover.tsp.core
import cats.kernel.Order
import ru.itclover.tsp.core.Pattern.Idx

import scala.annotation.tailrec
import scala.collection.{mutable => m}

object QueueUtils {

  private val trueFunction = (x: Any) => true

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

  @tailrec
  def rollMap(idx: Idx, q: m.Queue[(Idx, Time)])(implicit ord: Order[Idx]): (Time, m.Queue[(Idx, Time)]) = {
    assert(q.nonEmpty, "IdxTimeMap should not be empty!") // .. understandable exception?
    q.dequeue match {
      case (index, time) =>
        if (ord.eqv(index, idx)) (time, q)
        else if (ord.lt(index, idx)) rollMap(idx, q)
        else sys.error("Invalid state!") // .. same?
    }
  }
}
