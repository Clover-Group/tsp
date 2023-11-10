package ru.itclover.tsp.core.patterns

import java.time.Instant

import cats.Id
import org.scalatest.wordspec._

import org.scalatest.matchers.should._
import ru.itclover.tsp.core.{IdxValue, Patterns, StateMachine}
import ru.itclover.tsp.core.fixtures.Common.EInt
import ru.itclover.tsp.core.fixtures.Event
import ru.itclover.tsp.core.utils.TimeSeriesGenerator.Increment
import ru.itclover.tsp.core.utils.{RandomInRange, Timer}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.concurrent.duration._

// In test cases, 'should' expressions are non-unit. Suppressing wartremover warnings about it
@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class CouplePatternTest extends AnyWordSpec with Matchers {

  val p: Patterns[EInt] = Patterns[EInt]
  import p._

  "Couple Pattern" should {

    "run simple pattern" in {

      val pattern = p.assert(field(_.row) > const(0))

      // val rnd: Random = new Random()

      val events = (for (
        time <- Timer(from = Instant.now());
        idx  <- Increment;
        row  <- RandomInRange(0, 10)(new Random()).timed(10.seconds)
      )
        yield Event[Int](time.toEpochMilli, idx.toLong, row.toInt, 0)).run(seconds = 10)

      val out = new ArrayBuffer[IdxValue[_]]()
      val _ = StateMachine[Id].run(pattern, events, pattern.initialState(), (x: IdxValue[_]) => out += x, 1)

      out.count(_.value.isFail) !== 0

    }

  }

}
