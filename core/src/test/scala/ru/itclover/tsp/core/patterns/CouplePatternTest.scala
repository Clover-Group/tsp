package ru.itclover.tsp.core.patterns

import java.time.Instant

import cats.Id
import org.scalatest.{Matchers, WordSpec}
import ru.itclover.tsp.core.{IdxValue, Patterns, StateMachine}
import ru.itclover.tsp.core.fixtures.Common.EInt
import ru.itclover.tsp.core.fixtures.Event
import ru.itclover.tsp.core.utils.TimeSeriesGenerator.Increment
import ru.itclover.tsp.core.utils.{Constant, RandomInRange, TimeSeriesGenerator, Timer}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.concurrent.duration._

class CouplePatternTest extends WordSpec with Matchers {

  val p: Patterns[EInt] = Patterns[EInt]
  import p._

  "Couple Pattern" should {

    "run simple pattern" in {

      val pattern = p.assert(field(_.row) > const(0))

      val events = (for (time <- Timer(from = Instant.now());
                         idx  <- Increment;
                         row  <- RandomInRange(0, 10)(new Random()).timed(10.seconds))
        yield Event[Int](time.toEpochMilli, idx.toLong, row.toInt, 0)).run(seconds = 10)

      val out = new ArrayBuffer[IdxValue[_]]()
      StateMachine[Id].run(pattern, events, pattern.initialState(), (x: IdxValue[_]) => out += x, 1)

      out.count(_.value.isSuccess) shouldNot be(0)
    }

    "'or' works" in {

      val pattern = p.assert(field(_.row == 0).or(field(_.col == 0)))

      val events = (for (time <- Timer(from = Instant.now());
                         idx  <- Increment;
                         row  <- Constant(0).timed(1.seconds).after(Constant(1));
                         col  <- Constant(1).timed(1.seconds).after(Constant(0).timed(1.seconds)).after(Constant(1)))
        yield Event[Int](time.toEpochMilli, idx.toLong, row, col)).run(seconds = 10)

      val out = new ArrayBuffer[IdxValue[_]]()
      StateMachine[Id].run(pattern, events, pattern.initialState(), (x: IdxValue[_]) => out += x, 1)

      out.count(_.value.isSuccess) shouldBe 2
    }

  }

}
