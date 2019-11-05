package ru.itclover.tsp.core.aggregators

import java.time.Instant

import cats.Id
import org.scalatest.{Matchers, WordSpec}
import ru.itclover.tsp.core.Time._
import ru.itclover.tsp.core.fixtures.Common.EInt
import ru.itclover.tsp.core.fixtures.Event
import ru.itclover.tsp.core.utils.TimeSeriesGenerator.Increment
import ru.itclover.tsp.core.utils.{Constant, Timer}
import ru.itclover.tsp.core.{IdxValue, Patterns, StateMachine}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

class GroupPatternTest extends WordSpec with Matchers {

  "GroupPatternTest" should {
    "calculate sum for successes" in {
      val pat = Patterns[EInt]
      import pat._

      val innerPattern = const(1) // returns Success(1) for any sequence of events

      val events = (for (time <- Timer(from = Instant.now());
                         idx  <- Increment;
                         row  <- Constant(0))
        yield Event[Int](time.toEpochMilli, idx.toLong, row, 0)).run(seconds = 100)

      import cats.instances.int.catsKernelStdGroupForInt

      val groupPattern = GroupPattern(innerPattern, 10.seconds).map(_.sum)

      val collect = new ArrayBuffer[IdxValue[Int]]()
      StateMachine[Id].run(groupPattern, events, groupPattern.initialState(), (x: IdxValue[Int]) => collect += x)

      collect.size shouldBe 100
      collect.foreach(x => {
        x.value.isSuccess shouldBe true
        x.value.getOrElse(100) < 11 shouldBe true
      })

    }

    //todo
    "don't fail if there is only few Fails in a window" in {}

    //todo
    "fail if all events in window are Fails" in {}

  }

}
