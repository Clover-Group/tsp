package ru.itclover.tsp.core.timeseries

import java.time.temporal.ChronoUnit
import java.time.{Instant, Duration => JavaDuration}
import java.util.Random

import cats.Id
import org.scalatest.{Matchers, WordSpec}
import ru.itclover.tsp.core.fixtures.Common.EInt
import ru.itclover.tsp.core.fixtures.Event
import ru.itclover.tsp.core._
import ru.itclover.tsp.core.utils.TimeSeriesGenerator.Increment
import ru.itclover.tsp.core.utils.{Change, Constant, RandomInRange, TimeSeriesGenerator, Timer}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.language.reflectiveCalls
import scala.concurrent.duration._

class GeneratorTest extends WordSpec with Matchers {

  def process(e: EInt): Long = e.row

  "test-time-series" should {

    implicit val random: Random = new Random(100)

    val expectedData = SimplePState(PQueue.empty)

    "match-for-valid-1" in {
      val patterns = new ArrayBuffer[SimplePattern[EInt, Int]]()

      val events = (for (time <- Timer(from = Instant.now());
                         idx  <- Increment;
                         pump <- RandomInRange(1, 100)(random).map(_.toDouble).timed(40.seconds).after(Constant(0));
                         speed <- Constant(261.0)
                           .timed(1.second)
                           .after(Change(from = 260.0, to = 0.0, howLong = 10.seconds))
                           .after(Constant(0.0)))
        yield Event[Int](time.getEpochSecond, idx, speed.toInt, pump.toInt)).run(seconds = 100)

      events
        .foreach(
          event => patterns.append(new SimplePattern[EInt, Int](_ => Result.succ(process(event).toInt)))
        )

      val result = (patterns, events).zipped.map { (p, e) =>
        StateMachine[Id].run(p, Seq(e), p.initialState())
      }

      result(0).queue.size shouldBe 0

    }

    "match for valid-2" in {
      val patterns = new ArrayBuffer[SimplePattern[EInt, Int]]()
      val events = (for (time <- Timer(from = Instant.now());
                         idx  <- Increment;
                         pump <- RandomInRange(1, 100)(random).map(_.toDouble).timed(40.seconds).after(Constant(0));
                         speed <- Change(from = 1.0, to = 261, 15.seconds)
                           .timed(1.seconds)
                           .after(Change(from = 260.0, to = 0.0, howLong = 10.seconds))
                           .after(Constant(0.0)))
        yield Event[Int](time.getEpochSecond, idx, speed.toInt, pump.toInt)).run(seconds = 100)

      events
        .foreach(
          event => patterns.append(new SimplePattern[EInt, Int](_ => Result.succ(process(event).toInt)))
        )

      val result = (patterns, events).zipped.map { (p, e) =>
        StateMachine[Id].run(p, Seq(e), p.initialState())
      }

      result(0).queue.size shouldBe 0
    }

    "not to match" in {
      val patterns = new ArrayBuffer[SimplePattern[EInt, Int]]()

      val events = (for (time <- Timer(from = Instant.now());
                         idx  <- Increment;
                         pump <- RandomInRange(1, 100)(random).map(_.toDouble).timed(40.seconds).after(Constant(0));
                         speed <- Constant(250d)
                           .timed(1.second)
                           .after(Change(from = 250.0, to = 0.0, howLong = 10.seconds))
                           .after(Constant(0.0)))
        yield Event[Int](time.getEpochSecond, idx, speed.toInt, pump.toInt)).run(seconds = 100)

      events
        .foreach(
          event => patterns.append(new SimplePattern[EInt, Int](_ => Result.succ(process(event).toInt)))
        )

      val result = (patterns, events).zipped.map { (p, e) =>
        StateMachine[Id].run(p, Seq(e), p.initialState())
      }

      result(0).queue.size shouldBe 0
    }

  }

  "customTest" should {

    implicit val random: Random = new java.util.Random(345L)
    val expectedData = SimplePState(PQueue.empty)

    "match" in {
      val patterns = new ArrayBuffer[SimplePattern[EInt, Int]]()

      val events = (for (time <- Timer(from = Instant.now());
                         idx  <- Increment;
                         pump <- RandomInRange(1, 100)(random).map(_.toDouble).timed(40.seconds).after(Constant(0));
                         speed <- Constant(250d)
                           .timed(1.second)
                           .after(Change(from = 250.0, to = 0.0, howLong = 30.seconds))
                           .after(Constant(0.0)))
        yield Event[Int](time.getEpochSecond, idx, speed.toInt, pump.toInt)).run(seconds = 100)

      events
        .foreach(
          event => patterns.append(new SimplePattern[EInt, Int](_ => Result.succ(process(event).toInt)))
        )

      val result = (patterns, events).zipped.map { (p, e) =>
        StateMachine[Id].run(p, Seq(e), p.initialState())
      }

      result(0).queue.size shouldBe 0
    }
  }

}

object GeneratorTest extends App {}
