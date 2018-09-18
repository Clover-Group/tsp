package ru.itclover.tsp.core

import org.scalatest.{Matchers, WordSpec}
import scala.collection.{ mutable => m }
import ru.itclover.tsp.utils.CollectionsOps.MutableQueueOps


class DequeueWhileTest extends WordSpec with Matchers {

  "dequeueWhile" should {

    "Dequeue nothing in empty queue" in {
      val emptyQ = m.Queue.empty[Int]
      val deq = emptyQ.dequeueWhile(_ => true)
      deq.size shouldEqual 0
      emptyQ.size shouldEqual 0
    }

    "Dequeue one element queue" in {
      val q = m.Queue(1)
      val deq = q.dequeueWhile(_ => true)
      deq.headOption shouldEqual Some(1)
      q.size shouldEqual 0
    }

    "Dequeue some elements by predicate" in {
      val initital: Seq[Int] = Vector(1, 2, 3, 4, 5)
      val q = m.Queue(initital:_*)
      val deq = q.dequeueWhile { i => i % 2 == 1 }
      deq.headOption shouldEqual Some(1)
      q.zip(Vector(2, 3, 4, 5)) foreach { case (real, expected) =>
        real shouldEqual expected
      }
    }

    "Dequeue all elements queue" in {
      val expectedDeq: Seq[Int] = Vector(1, 2, 3, 4, 5)
      val q = m.Queue(expectedDeq:_*)
      val deq = q.dequeueWhile(_ => true)
      deq.size shouldEqual expectedDeq.size
      deq.zip(expectedDeq) foreach { case (real, expected) =>
        real shouldEqual expected
      }
      q.size shouldEqual 0
    }

    "Dequeue all elements big queue" in {
      val expectedDeq: Seq[Int] = Range(3, 3333, 1)
      val q = m.Queue(expectedDeq:_*)
      val deq = q.dequeueWhile(_ => true)
      deq.size shouldEqual expectedDeq.size
      deq.zip(expectedDeq) foreach { case (real, expected) =>
        real shouldEqual expected
      }
      q.size shouldEqual 0
    }

    "Dequeue nothing on false predicate" in {
      val initial = Vector(1, 2, 3, 4, 5)
      val q = m.Queue(initial:_*)
      val deq = q.dequeueWhile(_ => false)
      deq.size shouldEqual 0
      q.size shouldEqual initial.size
      q.zip(initial) foreach { case (real, expected) =>
          real shouldEqual expected
      }
    }

  }
}
