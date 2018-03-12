package ru.itclover.streammachine.core

import org.scalatest.WordSpec
import ru.itclover.streammachine.core.PhaseResult.{Failure, Stay, Success}
import ru.itclover.streammachine.aggregators.AggregatorPhases._
import scala.concurrent.duration._
import ru.itclover.streammachine.core.Time._
import ru.itclover.streammachine.core.PhaseParser.Functions._
import ru.itclover.streammachine.phases.NumericPhases.NumericPhaseParser
import ru.itclover.streammachine.utils.ParserMatchers


class AccumulationParsersTest extends WordSpec with ParserMatchers {

  "SumParser" should {
    "work on stay and success events" in {
      checkOnTestEvents(
        (p: TestPhase[Double]) => Sum(p, 2.seconds),
        staySuccesses,
        Seq(Success(3.0), Success(3.0), Success(6.0), Success(6.0), Failure("Test"), Failure("Test"), Failure("Test"))
      )
    }
    "not work on fail-interleaved events" in {
      checkOnTestEvents(
        (p: TestPhase[Double]) => Sum(p, 2.seconds),
        fails,
        (0 until 10).map(_ => Failure("Test"))
      )
    }
  }

  "AvgParser" should {
    "work on stay-success" in {
      checkOnTestEvents(
        (p: TestPhase[Double]) => Average(p, 2.seconds),
        staySuccesses,
        Seq(Success(1.5), Success(1.5), Success(2.0), Success(2.0), Failure("Test"), Failure("Test"), Failure("Test")),
        0.001
      )
    }

    "not work on fail-interleaved events" in {
      checkOnTestEvents(
        (p: TestPhase[Double]) => Average(p, 2.seconds),
        fails,
        (0 until 10).map(_ => Failure("Test"))
      )
    }
  }

  "CountParser" should {
    "work on stay-success" in {
      checkOnTestEvents(
        (p: TestPhase[Double]) => Count(p, 2.seconds),
        staySuccesses,
        Seq(Success(2.0), Success(2.0), Success(3.0), Success(3.0), Failure("Test"), Failure("Test"), Failure("Test"))
      )
    }

    "not work on fail-interleaved events" in {
      checkOnTestEvents(
        (p: TestPhase[Double]) => Count(p, 2.seconds),
        fails,
        (0 until 10).map(_ => Failure("Test"))
      )
    }
  }
}
