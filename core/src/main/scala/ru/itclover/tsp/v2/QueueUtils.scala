package ru.itclover.tsp.v2
import ru.itclover.tsp.core.Time
import ru.itclover.tsp.v2.Extract.Idx

import scala.annotation.tailrec
import scala.collection.{mutable => m}

object  QueueUtils {

  private val trueFunction = (x: Any) => true

  def takeWhileFromQueue[A](queue:  m.Queue[A])(predicate: A => Boolean = trueFunction):  (m.Queue[A],  m.Queue[A]) = {
    if (predicate.eq(trueFunction)) (queue,  m.Queue.empty)
    else {

      @tailrec
      def inner(result:  m.Queue[A], q:  m.Queue[A]):  (m.Queue[A],  m.Queue[A]) =
        q.headOption match {
          case Some(x) if predicate(x) => inner({result.enqueue(x); result}, {q.dequeue(); q})
          case _                                   => (result, q)
        }

      inner (m.Queue.empty, queue)
    }
  }

  @tailrec
  def rollMap(idx: Idx, q:  m.Queue[(Idx, Time)]): (Time,  m.Queue[(Idx, Time)]) = {
    assert(q.nonEmpty, "IdxTimeMap should not be empty!")
    q.dequeue match {
      case ((index, time)) =>
        if (index == idx) (time, q)
        else if (index < idx) rollMap(idx, q)
        else sys.error("Invalid state!")
    }
  }
}
