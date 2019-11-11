package ru.itclover.tsp.core.patterns

import java.time.Instant

import cats.Id
import org.scalatest.{FlatSpec, Matchers}
import ru.itclover.tsp.core.fixtures.Common.EInt
import ru.itclover.tsp.core.fixtures.Event
import ru.itclover.tsp.core.utils.TimeSeriesGenerator.Increment
import ru.itclover.tsp.core.utils.{Change, Constant, Timer}
import ru.itclover.tsp.core.{IdxValue,Patterns, StateMachine}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

class AndThenPatternTest extends FlatSpec with Matchers {

  val p: Patterns[EInt] = Patterns[EInt]
  import p._

  it should "combine two simple patterns" in {

    val pattern = p.assert(field(_.row) > const(0)).andThen(p.assert(field(_.col) =!= const(0)))

    val events = (for (time <- Timer(from = Instant.now());
                       idx  <- Increment;
                       row  <- Change(from = 0.0, to = 100.0, 100.seconds).after(Constant(1)))
      yield Event[Int](time.toEpochMilli, idx.toLong, row.toInt, 0)).run(seconds = 100)

    val out = new ArrayBuffer[IdxValue[_]]()
    StateMachine[Id].run(pattern, events, pattern.initialState(), (x: IdxValue[_]) => out += x, 1)
    out.size shouldBe 100
  }

  it should "parse combination of and, andThen in rule" in {

    val pattern = p
      .assert(
        (
          field(_.row) > const(10)
        ).and(
          field(_.ts) > const(1000)
        )
      )
      .andThen(
        p.assert(
          field(_.col) =!= const(0)
        )
      )

    val events = (for (time <- Timer(from = Instant.now());
                       idx  <- Increment;
                       row  <- Change(from = 0.0, to = 100.0, 100.seconds).after(Constant(1)))
      yield Event[Int](time.toEpochMilli, idx.toLong, row.toInt, 0)).run(seconds = 100)

    val out = new ArrayBuffer[IdxValue[_]]()
    StateMachine[Id].run(pattern, events, pattern.initialState(), (x: IdxValue[_]) => out += x, 1)

    out.size shouldBe 110

  }

}
