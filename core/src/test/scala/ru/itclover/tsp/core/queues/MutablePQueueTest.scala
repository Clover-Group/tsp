package ru.itclover.tsp.core.queues

import org.scalatest.wordspec._

import org.scalatest.matchers.should._
import ru.itclover.tsp.core.PQueue.MutablePQueue
import ru.itclover.tsp.core.{IdxValue, PQueue, Result, Succ}

import scala.collection.mutable

/**
  * Test class for mutable pattern queue
  */
// In test cases, 'should' expressions are non-unit. Suppressing wartremover warnings about it
@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class MutablePQueueTest extends AnyWordSpec with Matchers {

  "mutable pattern queue" should {

    val testQueue = MutablePQueue[Int]()

    (0 to 1000)
      .foreach(i => testQueue.enqueue(IdxValue(i.toLong, i.toLong, Result.succ(i))))

    "retrieve head option" in {

      val expectedData = Result.succ(0)
      val actualData = testQueue.headOption.map(_.value).getOrElse(Result.fail)

      actualData shouldBe expectedData

    }

    "dequeue" in {

      val expectedData = 1000
      val actualData = testQueue.dequeue()._2.size

      actualData shouldBe expectedData

    }

    "retrieve dequeue option" in {

      val expectedData = Result.succ(1)
      val actualData = testQueue.dequeueOption().map(_._1.value).getOrElse(Result.fail)

      actualData shouldBe expectedData

    }

    "behead" in {

      val tempQueue = testQueue.behead()

      val expectedData = Result.succ(3)
      val actualData = tempQueue.headOption.map(_.value).getOrElse(Result.fail)

      actualData shouldBe expectedData

    }

    "retrieve behead option" in {

      val tempQueue = testQueue.beheadOption().getOrElse(PQueue.empty)

      val expectedData = Result.succ(4)
      val actualData = tempQueue.headOption.map(_.value).getOrElse(Result.fail)

      actualData shouldBe expectedData

    }

    "enqueue" in {

      testQueue.enqueue(IdxValue(1, 1, Result.succ(1)))

      val expectedData = Result.succ(4)
      val actualData = testQueue.dequeueOption().map(_._1.value).getOrElse(Result.fail)

      actualData shouldBe expectedData

    }

    "clean" in {

      val cleanResult = testQueue.clean()
      cleanResult.enqueue(IdxValue(1, 1, Result.succ(1)))

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

    def getTestQueue: MutablePQueue[Int] = {
      val testQueue = MutablePQueue[Int]()

      (0 to 1000)
        .foreach(i => testQueue.enqueue(IdxValue(2 * i.toLong, 2 * i.toLong + 1, Result.succ(i))))

      testQueue
    }

    "rewindTo-1" in {
      getTestQueue.rewindTo(0).size shouldBe 1001
      getTestQueue.rewindTo(1).size shouldBe 1001
      getTestQueue.rewindTo(1).headOption.map(_.start) shouldBe Some(1)
      getTestQueue.rewindTo(23).headOption.map(_.start) shouldBe Some(23)
      getTestQueue.rewindTo(100000).size shouldBe 0
    }

  }

}
