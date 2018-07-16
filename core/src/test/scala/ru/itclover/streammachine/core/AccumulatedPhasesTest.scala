package ru.itclover.streammachine.core

import org.scalatest.{Matchers, WordSpec}
import ru.itclover.streammachine.aggregators.{Aligned, Skip}
import ru.itclover.streammachine.core.PhaseResult.{Failure, Stay, Success, TerminalResult}
import ru.itclover.streammachine.aggregators.AggregatorPhases._
import scala.concurrent.duration._
import ru.itclover.streammachine.core.Time._
import ru.itclover.streammachine.core.PhaseParser.Functions._
import ru.itclover.streammachine.phases.{ConstantPhases, NoState}
import ru.itclover.streammachine.phases.NumericPhases.NumericPhaseParser
import ru.itclover.streammachine.utils.ParserMatchers
import scala.language.implicitConversions


class AccumulatedPhasesTest extends WordSpec with ParserMatchers with Matchers {

  "Skip phase" should {
    "skip on avg phases" in {
      val rangeRes = Seq(Success(1.0), Success(2.0), Success(3.0), Success(4.0), Success(5.0))
      val simpleRange = for((t, res) <- times.take(rangeRes.length).zip(rangeRes)) yield TestingEvent(res, t)
      val expectedResults = Seq(Success(7.0))
      checkOnTestEvents(
                                  // (3 + 4 + 5) / 3 + (1 + 2 + 3 + 4 + 5) / 5 = 7.0
        (p: TestPhase[Double]) => Skip(2, avg(p, 2.seconds)) plus avg(p, 4.seconds),
        simpleRange,
        expectedResults,
        Some(0.0001)
      )
    }

    "not skip for empty padding" in {
      val rangeRes = Seq(Success(1.0), Success(2.0), Success(3.0), Success(4.0), Success(5.0))
      val simpleRange = for((t, res) <- times.take(rangeRes.length).zip(rangeRes)) yield TestingEvent(res, t)
      val expectedResults = Seq(Success(6.0))
      val a = intercept[Exception] { checkOnTestEvents(
        (p: TestPhase[Double]) => Skip(0, avg(p, 4.seconds)) plus avg(p, 4.seconds),
        simpleRange,
        expectedResults
      ) }
      a.isInstanceOf[IllegalArgumentException] shouldBe true
    }
  }

  "Aligned phase" should {
    "align avg phases" in {
      val rangeRes = Seq(Success(1.0), Success(2.0), Success(3.0), Success(4.0), Success(5.0))
      val simpleRange = for((t, res) <- times.take(rangeRes.length).zip(rangeRes)) yield TestingEvent(res, t)
      val expectedResults = Seq(Success(7.0))
      checkOnTestEvents(
                                  // (3 + 4 + 5) / 3 + (1 + 2 + 3 + 4 + 5) / 5 = 7.0
        (p: TestPhase[Double]) => Aligned(2.seconds, avg(p, 2.seconds)) plus avg(p, 4.seconds),
        simpleRange,
        expectedResults,
        Some(0.0001)
      )
    }

    "not align for empty padding" in {
      val rangeRes = Seq(Success(1.0), Success(2.0), Success(3.0), Success(4.0), Success(5.0))
      val simpleRange = for((t, res) <- times.take(rangeRes.length).zip(rangeRes)) yield TestingEvent(res, t)
      val expectedResults = Seq(Success(6.0))
      val a = intercept[Exception] { checkOnTestEvents(
        (p: TestPhase[Double]) => Aligned(0.seconds, avg(p, 4.seconds)) plus avg(p, 4.seconds),
        simpleRange,
        expectedResults
      ) }
      a.isInstanceOf[IllegalArgumentException] shouldBe true
    }
  }

  "SumParser" should {
    "work on stay and success events" in {
      checkOnTestEvents(
        (p: TestPhase[Double]) => sum(p, 2.seconds),
        staySuccesses,
        Seq(Success(3.0), Success(3.0), Success(6.0), Success(6.0), Failure("Test"), Failure("Test"), Failure("Test"))
      )
    }
    "not work on fail-interleaved events" in {
      checkOnTestEvents(
        (p: TestPhase[Double]) => sum(p, 2.seconds),
        fails,
        (0 until 10).map(_ => Failure("Test"))
      )
    }
  }

  "AvgParser" should {
    "work on stay-success" in {
      checkOnTestEvents(
        (p: TestPhase[Double]) => avg(p, 2.seconds),
        staySuccesses,
        Seq(Success(1.5), Success(1.5), Success(2.0), Success(2.0), Failure("Test"), Failure("Test"), Failure("Test")),
        Some(0.001)
      )
    }

    "not work on fail-interleaved events" in {
      checkOnTestEvents(
        (p: TestPhase[Double]) => avg(p, 2.seconds),
        fails,
        (0 until 10).map(_ => Failure("Test"))
      )
    }
  }

  "CountParser" should {
    "work on stay-success" in {
      checkOnTestEvents(
        (p: TestPhase[Double]) => count(p, 2.seconds),
        staySuccesses,
        // Note: accumulate only Successes, not Stays
        Seq(Success(2L), Success(2L), Success(3L), Success(3L), Failure("Test"), Failure("Test"), Failure("Test"))
      )
    }

    "work on bool staySuccesses" in {
      checkOnTestEvents_strict(
        (p: TestPhase[Boolean]) => count(p, 2.seconds),
        staySuccesses map (t => TestingEvent(t.result.map(_ > 1.0), t.time)),
        Seq(Success(2L), Success(2L), Success(3L), Success(3L), Failure("Test"), Failure("Test"), Failure("Test"))
      )
    }

    "not work on fail-interleaved events" in {
      checkOnTestEvents(
        (p: TestPhase[Double]) => count(p, 2.seconds),
        fails,
        (0 until 10).map(_ => Failure("Test"))
      )
    }
  }

  "millisCount phase" should {
    "work on staySuccesses" in {
      checkOnTestEvents(
        (p: TestPhase[Double]) => millisCount(p, 2.seconds),
        staySuccesses,
        // Note: phase skipping Stay results, hence fewer successes
        Seq(Success(2000L), Success(2000L), Success(2000L), Success(2000L), Failure("Test"), Failure("Test"), Failure("Test"))
      )
    }
    "not work on fails" in {
      checkOnTestEvents(
        (p: TestPhase[Double]) => millisCount(p, 2.seconds),
        fails,
        (0 until 10).map(_ => Failure("Test"))
      )
    }
  }

  "truthCount phase" should {
    "work on staySuccesses" in {
      checkOnTestEvents_strict(
        (p: TestPhase[Boolean]) => truthCount(p, 2.seconds),
        staySuccesses map (t => TestingEvent(t.result.map(_ > 1.0), t.time)),
        // Note: phase skipping Stay results, hence fewer successes
        Seq(Success(1L), Success(1L), Success(2L), Success(2L), Failure("Test"), Failure("Test"), Failure("Test"))
      )
    }
    "not work on fails" in {
      checkOnTestEvents_strict(
        (p: TestPhase[Boolean]) => truthCount(p, 2.seconds),
        fails map (t => TestingEvent(t.result.map(_ > 1.0), t.time)),
        (0 until 10).map(_ => Failure("Test"))
      )
    }
  }

  "truthMillisCount phase" should {
    "work on staySuccesses" in {
      implicit def longToPhase(num: Long) = ConstantPhases[TestPhase[Boolean], Long](num)
      val phase = (p: TestPhase[Boolean]) => truthMillisCount(p, 2.seconds)
      checkOnTestEvents_strict(
        phase,
        getTestingEvents(Seq(Success(true), Success(false), Success(true), Success(true), Success(true))),
        Seq(Success(2000L), Success(2000L), Success(3000L))
      )

      checkOnTestEvents_strict(
        (p: TestPhase[Boolean]) => truthMillisCount(p, 2.seconds),
        staySuccesses map (t => TestingEvent(t.result.map(_ > 1.0), t.time)),
        // Note: phase skipping Stay results, hence fewer successes
        Seq(Success(2000L), Success(2000L), Success(2000L), Success(2000L), Failure("Test"), Failure("Test"), Failure("Test"))
      )
    }
    "not work on fails" in {
      checkOnTestEvents_strict(
        (p: TestPhase[Boolean]) => truthCount(p, 2.seconds),
        fails map (t => TestingEvent(t.result.map(_ > 1.0), t.time)),
        (0 until 10).map(_ => Failure("Test"))
      )
    }
  }
}
