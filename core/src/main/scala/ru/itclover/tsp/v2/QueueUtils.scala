package ru.itclover.tsp.v2
import ru.itclover.tsp.core.Time
import ru.itclover.tsp.v2.Extract.Idx

import scala.annotation.tailrec
import scala.collection.immutable.Queue

object QueueUtils {

  private val trueFunction = (x: Any) => true

  def takeWhileFromQueue[A](queue: Queue[A])(predicate: A => Boolean = trueFunction): (Queue[A], Queue[A]) = {
    if (predicate.eq(trueFunction)) (queue, Queue.empty)
    else {

      @tailrec
      def inner(result: Queue[A], q: Queue[A]): (Queue[A], Queue[A]) =
        q.dequeueOption match {
          case Some((x, newQueue)) if predicate(x) => inner(result.enqueue(x), newQueue)
          case _                                   => (result, q)
        }

      inner(Queue.empty, queue)
    }
  }

  @tailrec
  def rollMap(idx: Idx, q: Queue[(Idx, Time)]): (Time, Queue[(Idx, Time)]) = {
    assert(q.nonEmpty, "IdxTimeMap should not be empty!")
    q.dequeue match {
      case ((index, time), newq) =>
        if (index == idx) (time, newq)
        else if (index < idx) rollMap(idx, newq)
        else sys.error("Invalid state!")
    }
  }
}
