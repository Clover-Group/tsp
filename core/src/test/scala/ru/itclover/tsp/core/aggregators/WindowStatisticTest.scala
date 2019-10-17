package ru.itclover.tsp.core.aggregators

import java.time.Instant

import cats.Id
import org.scalatest.{Matchers, WordSpec}
import ru.itclover.tsp.core.fixtures.Common.EInt
import ru.itclover.tsp.core.fixtures.Event
import ru.itclover.tsp.core.{Fail, IdxValue, Patterns, StateMachine, Succ}
import ru.itclover.tsp.core.utils.{Constant, Timer}
import ru.itclover.tsp.core.utils.TimeSeriesGenerator.Increment

import scala.concurrent.duration._
import ru.itclover.tsp.core.Time._

import scala.collection.mutable.ArrayBuffer

class WindowStatisticTest extends WordSpec with Matchers {

  //todo tests for WindowStatistic
  val pat = Patterns[EInt]
  import pat._

  "window statistic pattern" should {

    "count truthMillis" in {

      val pattern = truthMillis(pat.assert(field(_.row) === const(0)), 10.seconds)

      val events = (for (time <- Timer(from = Instant.now());
                         idx  <- Increment;
                         row  <- Constant(0).timed(40.seconds).after(Constant(1)))
        yield Event[Int](time.toEpochMilli, idx, row, 0)).run(seconds = 100)
      val collect = new ArrayBuffer[IdxValue[_]]()
      val finalState = StateMachine[Id].run(pattern, events, pattern.initialState(), (x: IdxValue[_]) => collect += x)

      collect.size shouldBe 100
      collect(0) shouldBe IdxValue(0, 0, Succ(0))
      collect(10) shouldBe IdxValue(10, 10, Succ(10000))
      collect(45) shouldBe IdxValue(45, 45, Succ(5000))
      collect(70) shouldBe IdxValue(70, 70, Succ(0))
    }

  }

  //todo more tests

}
