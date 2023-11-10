package ru.itclover.tsp.core.queues

import org.scalatest.wordspec._

import org.scalatest.matchers.should._
import ru.itclover.tsp.core.PQueue.{MapPQueue, MutablePQueue}
import ru.itclover.tsp.core.{IdxValue, Result, Succ}

import scala.collection.mutable

/** Test class for lazy variant of PQueue
  */
class MapPQueueTest extends AnyWordSpec with Matchers {

  "lazy variant of pattern queue" should {

    val transferQueue = MutablePQueue[Int]()

    (0 to 20)
      .foreach(i => transferQueue.enqueue(IdxValue(i.toLong, i.toLong, Result.succ(i))))

    val testQueue = MapPQueue[Int, Int](transferQueue, (item => item.value))

    "return it's size" in {

      val expectedData = 21
      val actualData = testQueue.size

      actualData shouldBe expectedData

    }

    "retrieve head option" in {

      val expectedData = Succ(0)
      val actualData = testQueue.headOption.get.value

      actualData shouldBe expectedData

    }

    "dequeue" in {

      val expectedData = 20
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

    "not to enqueue" in {

      val expectedData = "Cannot enqueue to IdxMapPQueue! Bad logic"

      val thrownException = the[UnsupportedOperationException] thrownBy testQueue.enqueue(IdxValue(1, 1, Result.succ(1)))
      val actualData = thrownException.getMessage

      actualData shouldBe expectedData

    }

    "clean" in {

      val cleanResult = testQueue.clean()

      cleanResult.size shouldBe 0

    }

    "convert to sequence" in {

      val expectedResult = mutable.Queue[Succ[Int]]()

      (4 to 20)
        .foreach(item => expectedResult.enqueue(Succ(item)))

      val seq = testQueue.toSeq
      val actualResult = seq.map(item => item.value)

      actualResult shouldBe expectedResult

    }

  }

}
