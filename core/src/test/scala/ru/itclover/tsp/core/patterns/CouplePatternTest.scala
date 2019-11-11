package ru.itclover.tsp.core.patterns

import java.time.Instant

import cats.Id
import org.scalatest.{Matchers, WordSpec}
import ru.itclover.tsp.core.{IdxValue, Patterns, StateMachine}
import ru.itclover.tsp.core.fixtures.Common.EInt
import ru.itclover.tsp.core.fixtures.Event
import ru.itclover.tsp.core.utils.{RandomInRange, Timer}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.concurrent.duration._


class CouplePatternTest extends WordSpec with Matchers {

  val p: Patterns[EInt] = Patterns[EInt]
  import p._

  "Couple Pattern" should {

    "run simple pattern" in {

      val pattern = p.assert(field(_.row) > const(0))

      val rnd: Random = new Random()

      val events = (for (time <- Timer(from = Instant.now());
                         idx  <- RandomInRange(0, 10)(rnd);
                         row  <- RandomInRange(0, 10)(new Random()).timed(10.seconds))
        yield Event[Int](time.toEpochMilli, idx.toLong, row.toInt, 0)).run(seconds = 10)

      val out = new ArrayBuffer[IdxValue[_]]()
      StateMachine[Id].run(pattern, events, pattern.initialState(), (x: IdxValue[_]) => out += x, 1)

      out.count(_.value.isFail) !== 0

    }

  }

}
