package ru.itclover.tsp.phases

import java.time.{ZonedDateTime, Instant}
import java.time.format.DateTimeFormatter
import org.scalatest.{FunSuite, Matchers, WordSpec}
import ru.itclover.tsp.core
import ru.itclover.tsp.core.Pattern.Functions._
import ru.itclover.tsp.core.{PatternResult, TestPhase, Time, _}
import ru.itclover.tsp.core.PatternResult.{Failure, Stay, Success}
import ru.itclover.tsp.core.Time.TimeExtractor
import java.time.Instant
import ru.itclover.tsp.aggregators.AggregatorPhases._
import ru.itclover.tsp.core.Time._
import ru.itclover.tsp.phases.CombiningPhases.{And, TogetherParser}
import ru.itclover.tsp.phases.NumericPhases._
import ru.itclover.tsp.phases.TimePhases.Timer
import ru.itclover.tsp.utils.ParserMatchers

import scala.concurrent.duration._


class AggregatorsTest extends WordSpec with ParserMatchers {


  "Timer phase" should {
    "work on staySuccesses" in {
      checkOnTestEvents(
        (p: TestPhase[Double]) => p.timed(2.seconds, 2.seconds).map(_._1),
        staySuccesses,
        // Note: timer seeks here for 3 seconds range actually due to peculiarities of time comparison
        // Hence first failure due to expired timer.
        Seq(Failure(""), Success(2.0), Success(1.0), Success(3.0), Failure("Test"), Failure("Test"), Failure("Test"))
      )
    }
    "not work on fails" in {
      checkOnTestEvents(
        (p: TestPhase[Double]) => p.timed(2.seconds, 2.seconds).map(_._1),
        fails,
        (0 until 10).map(_ => Failure("Test"))
      )
    }
  }


  "Derivation phase" should {
    "work on staySuccesses" in {
      checkOnTestEvents(
        (p: TestPhase[Double]) => Derivation(p),
        staySuccesses,
        Seq(Success(0.0005), Success(0.0005), Success(-0.001), Success(-0.001), Success(0.002), Failure("Test"), Failure("Test"))
      )
    }
    "not work on fails" in {
      checkOnTestEvents(
        (p: TestPhase[Double]) => Derivation(p),
        fails,
        (0 until 10).map(_ => Failure(""))
      )
    }
  }

  "Delta phase" should {
    "work on staySuccesses" in {
      checkOnTestEvents(
        (p: TestPhase[Double]) => delta(p),
        staySuccesses,
        Seq(Success(1.0), Success(1.0), Success(-1.0), Success(-1.0), Success(2.0), Failure("Test"), Failure("Test"))
      )
    }
    "not work on fails" in {
      checkOnTestEvents(
        (p: TestPhase[Double]) => delta(p),
        fails,
        (0 until 10).map(_ => Failure(""))
      )
    }
  }

  "PreviousValue phase" should {
    "work on staySuccesses" in {
      checkOnTestEvents(
        (p: TestPhase[Double]) => lag(p),
        staySuccesses,
        Seq(Success(1.0), Success(1.0), Success(2.0), Success(2.0), Success(1.0), Failure("Test"), Failure("Test"))
      )
    }
    "not work on fails" in {
      checkOnTestEvents(
        (p: TestPhase[Double]) => lag(p),
        fails,
        (0 until 10).map(_ => Failure(""))
      )
    }
  }

  /*"IncludeStays phase" should {
    "work on stay-success" in {
      val wwsStream = Stay #:: Stay #:: Success(()) #:: Stream.empty[PatternResult[Unit]]
      val stay_success = ToSegments(StreamResult[TimedEvent, Unit](wwsStream))

      val events = times.take(3).map(t => TimedEvent(0, t))
      val results = runRule(stay_success, events)

      results.length should equal(3)
      results(0) should not be an [Success[_]]
      results(1) should not be an [Success[_]]
      results(2) shouldBe a [Success[_]]

      val segmentLengthOpt = results(2) match {
        case Success(Segment(from, to)) => Some(to.toMillis - from.toMillis)
        case _ => None
      }

      segmentLengthOpt should not be empty
      segmentLengthOpt.get should equal(2000L)
    }

    "not work on stay-failure" in {
      val wwfStream = Stay #:: Stay #:: Failure("Test failure") #:: Stream.empty[PatternResult[Unit]]
      val stay_failure = ToSegments(StreamResult[TimedEvent, Unit](wwfStream))

      val (result1, state1) = stay_failure.apply(TimedEvent(100, times.head), stay_failure.initialState)
      val (result2, state2) = stay_failure.apply(TimedEvent(200, times(1)), state1)
      val (result3, state3) = stay_failure.apply(TimedEvent(300, times(2)), state2)

      result1 should not be an [Failure]
      result2 should not be an [Failure]
      result3 shouldBe a [Failure]
    }
  }*/
}
