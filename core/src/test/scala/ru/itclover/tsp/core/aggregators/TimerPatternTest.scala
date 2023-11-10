package ru.itclover.tsp.core.aggregators

import java.time.Instant

import cats.Id
import org.scalatest.wordspec._

import org.scalatest.matchers.should._
import ru.itclover.tsp.core.Time._
import ru.itclover.tsp.core.fixtures.Common.EInt
import ru.itclover.tsp.core.fixtures.Event
import ru.itclover.tsp.core.utils.TimeSeriesGenerator.Increment
import ru.itclover.tsp.core.utils.{Constant, Timer}
import ru.itclover.tsp.core._

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

// In test cases, 'should' expressions are non-unit. Suppressing wartremover warnings about it
@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class TimerPatternTest extends AnyWordSpec with Matchers {

  val pat = Patterns[EInt]
  import pat._

  // `row` stays === 0 during 10 seconds.
  val pattern = timer(pat.assert(field(_.row) === const(0)), 10.seconds, 2000L)

  "timer pattern" should {

    "match-for-valid-1" in {

      val events = (for (
        time <- Timer(from = Instant.now());
        idx  <- Increment;
        row  <- Constant(0).timed(40.seconds).after(Constant(1))
      )
        yield Event[Int](time.toEpochMilli, idx.toLong, row, 0)).run(seconds = 100)
      val collect = new ArrayBuffer[IdxValue[Boolean]]()
      StateMachine[Id].run(pattern, events, pattern.initialState(), (x: IdxValue[Boolean]) => collect += x)

      // returns 2 intervals
      collect.size shouldBe 3
      collect(0) shouldBe IdxValue(0, 9, Fail)
      collect(1) shouldBe IdxValue(10, 39, Succ(true))
      collect(2) shouldBe IdxValue(40, 99, Fail)
    }

    "match-for-valid-2" in {

      val events = (for (
        time <- Timer(from = Instant.now());
        idx  <- Increment;
        row  <- Constant(1).timed(40.seconds).after(Constant(0))
      )
        yield Event[Int](time.toEpochMilli, idx.toLong, row, 0)).run(seconds = 100)
      val collect = new ArrayBuffer[IdxValue[Boolean]]()
      StateMachine[Id].run(pattern, events, pattern.initialState(), (x: IdxValue[Boolean]) => collect += x)

      // returns 2 intervals
//      collect.size shouldBe 2
      collect(0) shouldBe IdxValue(0, 49, Fail)
      collect(1) shouldBe IdxValue(50, 99, Succ(true))
    }
  }

}
