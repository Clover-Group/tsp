package ru.itclover.tsp.core.aggregators

import java.time.Instant

import cats.Id
import org.scalatest.{Matchers, WordSpec}
import ru.itclover.tsp.core.Time._
import ru.itclover.tsp.core.fixtures.Common.EInt
import ru.itclover.tsp.core.fixtures.Event
import ru.itclover.tsp.core.utils.TimeSeriesGenerator.Increment
import ru.itclover.tsp.core.utils.{Constant, Timer}
import ru.itclover.tsp.core.{IdxValue, Patterns, StateMachine, Succ, Time, Window}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

// In test cases, 'should' expressions are non-unit. Suppressing wartremover warnings about it
@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class WindowStatisticTest extends WordSpec with Matchers {

  //todo tests for WindowStatistic
  val pat = Patterns[EInt]
  import pat._

  "Window Statistic Pattern" should {

    "count truthMillis" in {

      val pattern = truthMillis(pat.assert(field(_.row) === const(0)), 10.seconds)

      val events = (for (time <- Timer(from = Instant.now());
                         idx  <- Increment;
                         row  <- Constant(0).timed(40.seconds).after(Constant(1)))
        yield Event[Int](time.toEpochMilli, idx.toLong, row, 0)).run(seconds = 100)
      val collect = new ArrayBuffer[IdxValue[_]]()
      StateMachine[Id].run(pattern, events, pattern.initialState(), (x: IdxValue[_]) => collect += x)

      collect.size shouldBe 100
      collect(0) shouldBe IdxValue(0, 0, Succ(0))
      collect(10) shouldBe IdxValue(10, 10, Succ(10000))
      collect(45) shouldBe IdxValue(45, 45, Succ(5000))
      collect(70) shouldBe IdxValue(70, 70, Succ(0))
    }

  }

  "Window Statistic Result" should {

    val testResult = WindowStatisticResult(
      idx = 1000,
      time = Time(250),
      lastWasSuccess = false,
      successCount = 10,
      successMillis = 1000,
      failCount = 5,
      failMillis = 500
    )

    "retrieve total millis" in {
      testResult.totalMillis shouldBe 1500
    }

    "retrieve total count" in {
      testResult.totalCount shouldBe 15
    }

    "plus change" in {

      val plusResult = testResult.plusChange(
        WindowStatisticQueueInstance(
          100,
          Time(100),
          false,
          150,
          150
        ),
        Window(100)
      )

      plusResult.failCount shouldBe 6
      plusResult.failMillis shouldBe 600

    }

    "minus change" in {

      val minusResult = testResult.minusChange(
        WindowStatisticQueueInstance(
          5,
          Time(50),
          false,
          300,
          200
        ),
        Window(100)
      )

      minusResult.failCount shouldBe 4
      minusResult.failMillis shouldBe 500

    }

  }

  //todo more tests

}
