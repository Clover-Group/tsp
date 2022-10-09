package ru.itclover.tsp.core.patterns

import java.time.Instant

import cats.Id
import org.scalatest.flatspec._

import org.scalatest.matchers.should._
import ru.itclover.tsp.core.fixtures.Common.EInt
import ru.itclover.tsp.core.fixtures.Event
import ru.itclover.tsp.core.utils.TimeSeriesGenerator.Increment
import ru.itclover.tsp.core.utils.{Change, Constant, Timer}
import ru.itclover.tsp.core.{IdxValue, Patterns, Result, StateMachine}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

// In test cases, 'should' expressions are non-unit. Suppressing wartremover warnings about it
@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class SimplePatternTest extends AnyFlatSpec with Matchers {

  val p = Patterns[EInt]
  import p._

  val pattern = field(_.row)

  private def runAndCollectOutput[A](events: Seq[Event[Int]]) = {
    val collect = new ArrayBuffer[IdxValue[Int]]()
    StateMachine[Id].run(pattern, events, pattern.initialState(), (x: IdxValue[Int]) => collect += x)
    collect
  }

  it should "return correct results for changing values" in {

    val events = (for (time <- Timer(from = Instant.now());
                       idx  <- Increment;
                       row  <- Change(from = 0.0, to = 100.0, 100.seconds).after(Constant(1)))
      yield Event[Int](time.toEpochMilli, idx.toLong, row.toInt, 0)).run(seconds = 100)

    val out = runAndCollectOutput(events)
    out.size shouldBe (100)
    out.foreach(x => x.start shouldBe (x.end))
  }

  it should "collect points to segments for same values" in {

    val events = (for (time <- Timer(from = Instant.now());
                       idx  <- Increment;
                       row  <- Constant(0).timed(10.seconds).after(Constant(1)))
      yield Event[Int](time.toEpochMilli, idx.toLong, row.toInt, 0)).run(seconds = 100)

    val out = runAndCollectOutput(events)
    out.size shouldBe (2)
    out(0) shouldBe (IdxValue(0, 9, Result.succ(0)))
    out(1) shouldBe (IdxValue(10, 99, Result.succ(1)))
  }

}
