package ru.itclover.tsp.core.patterns

import java.time.Instant

import cats.Id
import org.scalatest.{Matchers, WordSpec}
import ru.itclover.tsp.core.{IdxValue, Patterns, Result, StateMachine}
import ru.itclover.tsp.core.PQueue.MutablePQueue
import ru.itclover.tsp.core.fixtures.Common.EInt
import ru.itclover.tsp.core.fixtures.Event
import ru.itclover.tsp.core.utils.{Constant, RandomInRange, Timer}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.concurrent.duration._

//todo write tests
class CouplePatternTest extends WordSpec with Matchers {

  val p: Patterns[EInt] = Patterns[EInt]
  import p._

  "Couple Pattern" should {

    "run simple pattern" in {

      val testQueue1 = MutablePQueue[Int]()
      val testQueue2 = MutablePQueue[Int]()

      (0 to 1000)
        .foreach(i => {
          testQueue1.enqueue(IdxValue(i.toLong, i.toLong, Result.succ(i)))
          testQueue2.enqueue(IdxValue(i.toLong, i.toLong, Result.succ(i)))
        })

      val pattern = field(_.row).gt(const(0))

      val rnd: Random = new Random()

      val events = (for (time <- Timer(from = Instant.now());
                         idx  <- RandomInRange(0, 10)(rnd);
                         row  <- Constant(50).timed(40.seconds))
        yield Event[Int](time.toEpochMilli, idx.toLong, row.toInt, 0)).run(seconds = 40)

      val out = new ArrayBuffer[IdxValue[_]]()
      StateMachine[Id].run(pattern, events, pattern.initialState(), (x: IdxValue[_]) => x, 1)

      //FIXME: unrecognized behaviour
      out.size shouldBe 0

    }

  }

}
