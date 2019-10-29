package ru.itclover.tsp.core

import java.time.Instant

import cats.Id
import org.scalatest.{FlatSpec, FunSuite, Matchers, WordSpec}
import ru.itclover.tsp.core.fixtures.Common.EInt
import ru.itclover.tsp.core.fixtures.Event
import ru.itclover.tsp.core.utils.TimeSeriesGenerator.Increment
import ru.itclover.tsp.core.utils.{Change, Constant, Timer}

import scala.concurrent.duration._
import scala.collection.mutable.ArrayBuffer

class AndThenPatternTest extends FlatSpec with Matchers {

  val p = Patterns[EInt]
  import p._

  val pattern = (p.assert(field(_.row) > const(0))).andThen(p.assert(field(_.col) =!= const(0)))

  private def runAndCollectOutput[A](events: Seq[Event[Int]]) = {
    val collect = new ArrayBuffer[IdxValue[_]]()
    val state = StateMachine[Id].run(pattern, events, pattern.initialState(), (x: IdxValue[_]) => collect += x, 1)
    collect
  }

  it should "return correct results" in {

    val events = (for (time <- Timer(from = Instant.now());
                       idx  <- Increment;
                       row  <- Change(from = 0.0, to = 100.0, 100.seconds).after(Constant(1)))
      yield Event[Int](time.toEpochMilli, idx, row.toInt, 0)).run(seconds = 100)

    val out = runAndCollectOutput(events)
    out.size shouldBe (1)
  }

}
