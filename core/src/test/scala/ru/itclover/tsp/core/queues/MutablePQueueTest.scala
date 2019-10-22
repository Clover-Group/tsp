package ru.itclover.tsp.core.queues

import org.scalatest.{Matchers, WordSpec}
import ru.itclover.tsp.core.PQueue.MutablePQueue
import ru.itclover.tsp.core.{IdxValue, Result, Succ}

import scala.collection.mutable

/**
  * Test class for mutable pattern queue
  */
class MutablePQueueTest extends WordSpec with Matchers {

  "mutable pattern queue" should {

    val testQueue = MutablePQueue[Int](new mutable.Queue[IdxValue[Int]])

    (0 to 1000)
      .foreach(i => testQueue.enqueue(i.toLong, Result.succ(i)))

    "retrieve head option" in {

      val expectedData = Succ(0)
      val actualData = testQueue.headOption.get.value

      actualData shouldBe expectedData

    }

    "dequeue" in {

      val expectedData = 1000
      val actualData = testQueue.dequeue()._2.size

      actualData shouldBe expectedData

    }

    "retrieve dequeue option" in {

      val expectedData = Result.succ(1)
      val actualData = testQueue.dequeueOption().get._1.value

      actualData shouldBe expectedData

    }

    "behead" in {

      val tempQueue = testQueue.behead()

      val expectedData = Result.succ(3)
      val actualData = tempQueue.headOption.get.value

      actualData shouldBe expectedData

    }

    "retrieve behead option" in {

      val tempQueue = testQueue.beheadOption().get

      val expectedData = Result.succ(4)
      val actualData = tempQueue.headOption.get.value

      actualData shouldBe expectedData

    }

    "enqueue" in {

      testQueue.enqueue(1, Result.succ(1))

      val expectedData = Result.succ(4)
      val actualData = testQueue.dequeueOption().get._1.value

      actualData shouldBe expectedData

    }

    "clean" in {

      val cleanResult = testQueue.clean()
      cleanResult.enqueue(1, Result.succ(1))

      cleanResult.size shouldBe 1
    }

    "convert to sequence" in {

      val expectedResult = mutable.Queue[Succ[Int]]()

      (5 to 1000)
        .foreach(item => expectedResult.enqueue(Succ(item)))
      expectedResult.enqueue(Succ(1))

      val seq = testQueue.toSeq
      val actualResult = seq.map(item => item.value)

      actualResult shouldBe expectedResult

    }

    "return it's size" in {

      val expectedData = 997
      val actualData = testQueue.size

      actualData shouldBe expectedData

    }

  }

}
