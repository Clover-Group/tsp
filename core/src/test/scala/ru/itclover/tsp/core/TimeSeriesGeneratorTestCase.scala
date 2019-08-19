package ru.itclover.tsp.core

import java.time.temporal.ChronoUnit
import java.time.{Instant, Duration => JavaDuration}
import java.util.Random

import cats.Id
import org.scalatest.{Matchers, WordSpec}
import ru.itclover.tsp.core.Common.{EInt, extractor}
import ru.itclover.tsp.utils.{Change, Constant, RandomInRange, Timer}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.language.reflectiveCalls

class TimeSeriesGeneratorTestCase extends WordSpec with Matchers {

  def process(e: EInt): Long = e.row

  "test-time-series" should {

    implicit val random: Random = new Random(100)

    val expectedData = SimplePState(PQueue.empty)

    "match-for-valid-1" in {
      val patterns = new ArrayBuffer[SimplePattern[EInt, Int]]()

      val events = (for (time <- Timer(from = Instant.now());
                         pump <- RandomInRange(1, 100)(random)
                           .map(_.toDouble)
                           .timed(
                             Duration.fromNanos(
                               JavaDuration.of(40, ChronoUnit.SECONDS).toNanos
                             )
                           )
                           .after(Constant(0));
                         speed <- Constant(261.0)
                           .timed(
                             Duration.fromNanos(
                               JavaDuration.of(1, ChronoUnit.SECONDS).toNanos
                             )
                           )
                           .after(
                             Change(
                               from = 260.0,
                               to = 0.0,
                               howLong = Duration.fromNanos(
                                 JavaDuration.of(10, ChronoUnit.SECONDS).toNanos
                               )
                             )
                           )
                           .after(Constant(0.0)))
        yield Event[Int](time.getEpochSecond, speed.toInt, pump.toInt)).run(seconds = 100)

      events
        .foreach(
          event => patterns.append(new SimplePattern[EInt, Int](_ => Result.succ(process(event).toInt))(extractor))
        )

      val result = (patterns, events).zipped.map { (p, e) =>
        StateMachine[Id].run(p, Seq(e), p.initialState())
      }

      result(0) shouldBe expectedData

    }

    "match for valid-2" in {
      val patterns = new ArrayBuffer[SimplePattern[EInt, Int]]()
      val events = (for (time <- Timer(from = Instant.now());
                         pump <- RandomInRange(1, 100)(random)
                           .map(_.toDouble)
                           .timed(
                             Duration.fromNanos(
                               JavaDuration.of(40, ChronoUnit.SECONDS).toNanos
                             )
                           )
                           .after(Constant(0));
                         speed <- Change(
                           from = 1.0,
                           to = 261,
                           Duration.fromNanos(
                             JavaDuration.of(15, ChronoUnit.SECONDS).toNanos
                           )
                         ).timed(
                             Duration.fromNanos(
                               JavaDuration.of(1, ChronoUnit.SECONDS).toNanos
                             )
                           )
                           .after(
                             Change(
                               from = 260.0,
                               to = 0.0,
                               howLong = Duration.fromNanos(
                                 JavaDuration.of(10, ChronoUnit.SECONDS).toNanos
                               )
                             )
                           )
                           .after(Constant(0.0)))
        yield Event[Int](time.getEpochSecond, speed.toInt, pump.toInt)).run(seconds = 100)

      events
        .foreach(
          event => patterns.append(new SimplePattern[EInt, Int](_ => Result.succ(process(event).toInt))(extractor))
        )

      val result = (patterns, events).zipped.map { (p, e) =>
        StateMachine[Id].run(p, Seq(e), p.initialState())
      }

      result(0) shouldBe expectedData
    }

    "not to match" in {
      val patterns = new ArrayBuffer[SimplePattern[EInt, Int]]()

      val events = (for (time <- Timer(from = Instant.now());
                         pump <- RandomInRange(1, 100)(random)
                           .map(_.toDouble)
                           .timed(
                             Duration.fromNanos(
                               JavaDuration.of(40, ChronoUnit.SECONDS).toNanos
                             )
                           )
                           .after(Constant(0));
                         speed <- Constant(250d)
                           .timed(
                             Duration.fromNanos(
                               JavaDuration.of(1, ChronoUnit.SECONDS).toNanos
                             )
                           )
                           .after(
                             Change(
                               from = 250.0,
                               to = 0.0,
                               howLong = Duration.fromNanos(
                                 JavaDuration.of(10, ChronoUnit.SECONDS).toNanos
                               )
                             )
                           )
                           .after(Constant(0.0)))
        yield Event[Int](time.getEpochSecond, speed.toInt, pump.toInt)).run(seconds = 100)

      events
        .foreach(
          event => patterns.append(new SimplePattern[EInt, Int](_ => Result.succ(process(event).toInt))(extractor))
        )

      val result = (patterns, events).zipped.map { (p, e) =>
        StateMachine[Id].run(p, Seq(e), p.initialState())
      }

      result(0) shouldBe expectedData
    }

  }

  "customTest" should {

    implicit val random: Random = new java.util.Random(345L)
    val expectedData = SimplePState(PQueue.empty)

    "match" in {
      val patterns = new ArrayBuffer[SimplePattern[EInt, Int]]()

      val events = (for (time <- Timer(from = Instant.now());
                         pump <- RandomInRange(1, 100)(random)
                           .map(_.toDouble)
                           .timed(
                             Duration.fromNanos(
                               JavaDuration.of(40, ChronoUnit.SECONDS).toNanos
                             )
                           )
                           .after(Constant(0));
                         speed <- Constant(250d)
                           .timed(
                             Duration.fromNanos(
                               JavaDuration.of(1, ChronoUnit.SECONDS).toNanos
                             )
                           )
                           .after(
                             Change(
                               from = 250.0,
                               to = 0.0,
                               howLong = Duration.fromNanos(
                                 JavaDuration.of(30, ChronoUnit.SECONDS).toNanos
                               )
                             )
                           )
                           .after(Constant(0.0)))
        yield Event[Int](time.getEpochSecond, speed.toInt, pump.toInt)).run(seconds = 100)

      events
        .foreach(
          event => patterns.append(new SimplePattern[EInt, Int](_ => Result.succ(process(event).toInt))(extractor))
        )

      val result = (patterns, events).zipped.map { (p, e) =>
        StateMachine[Id].run(p, Seq(e), p.initialState())
      }

      result(0) shouldBe expectedData
    }
  }

}

object TimeSeriesGeneratorTestCase extends App {}
