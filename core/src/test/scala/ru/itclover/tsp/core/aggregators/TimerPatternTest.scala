package ru.itclover.tsp.core.aggregators

import java.time.Instant

import cats.Id
import org.scalatest.{Matchers, WordSpec}
import ru.itclover.tsp.core.Time._
import ru.itclover.tsp.core.fixtures.Common.EInt
import ru.itclover.tsp.core.fixtures.Event
import ru.itclover.tsp.core.utils.{Constant, Timer}
import ru.itclover.tsp.core.{IdxValue, Patterns, StateMachine}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.language.reflectiveCalls

class TimerPatternTest extends WordSpec with Matchers {

  val pat = Patterns[EInt]
  import pat._

  // `row` stays === 0 during 10 seconds.
  val pattern = timer(pat.assert(field(_.row) === const(0)), 10.seconds)

  "timer pattern" should {

    "match-for-valid-1" in {

      val events = (for (time <- Timer(from = Instant.now());
                         row  <- Constant(0).timed(40.seconds).after(Constant(1)))
        yield Event[Int](time.toEpochMilli, row, 0)).run(seconds = 100)
      val collect = new ArrayBuffer[IdxValue[Unit]]()
      StateMachine[Id].run(pattern, events, pattern.initialState(), (x: IdxValue[Unit]) => collect += x)

      collect.size shouldBe (0)
    }
  }

}
