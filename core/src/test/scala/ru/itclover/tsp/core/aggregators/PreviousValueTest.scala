package ru.itclover.tsp.core.aggregators

import java.time.Instant

import cats.Id
import org.scalatest.{Matchers, WordSpec}
import ru.itclover.tsp.core.Time._
import ru.itclover.tsp.core.fixtures.Common.EInt
import ru.itclover.tsp.core.fixtures.Event
import ru.itclover.tsp.core.utils.TimeSeriesGenerator.Increment
import ru.itclover.tsp.core.utils.Timer
import ru.itclover.tsp.core.{IdxValue, Patterns, StateMachine}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

// In test cases, 'should' expressions are non-unit. Suppressing wartremover warnings about it
@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class PreviousValueTest extends WordSpec with Matchers {
  "PreviousValue pattern" should {

    val pat = Patterns[EInt]
    import pat._
    val events = (for (time <- Timer(from = Instant.now());
                       idx  <- Increment;
                       row  <- Increment)
      yield Event[Int](time.toEpochMilli, idx.toLong, row, 0)).run(seconds = 100)

    "return prev values for success" in {

      val previousPattern = PreviousValue(field(_.row), 10.seconds)

      val collect = new ArrayBuffer[IdxValue[Int]]()
      StateMachine[Id].run(previousPattern, events, previousPattern.initialState(), (x: IdxValue[Int]) => collect += x)

      collect.size shouldBe 90
      collect.foreach(idxv => idxv.value.map(value => value + 10 shouldBe idxv.start).getOrElse(true shouldBe false))

    }

    "return fail if there was no successful value in past" in {
      val previousPattern = PreviousValue(pat.assert(const(0).equiv(const(1))), 10.seconds) // inner pattern returns Fals

      val collect = new ArrayBuffer[IdxValue[Unit]]()
      StateMachine[Id].run(previousPattern, events, previousPattern.initialState(), (x: IdxValue[Unit]) => collect += x)

      collect.size shouldBe 0

    }
  }

}
