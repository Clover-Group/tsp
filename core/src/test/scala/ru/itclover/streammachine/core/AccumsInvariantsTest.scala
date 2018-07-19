package ru.itclover.streammachine.core

import org.scalatest.{Matchers, WordSpec}
import ru.itclover.streammachine.aggregators.AggregatorPhases.{Aligned, Skip}
import ru.itclover.streammachine.core.PhaseResult.{Failure, Stay, Success, TerminalResult}
import ru.itclover.streammachine.aggregators.AggregatorPhases._
import ru.itclover.streammachine.aggregators.accums.{AccumPhase, OneTimeStates, ContinuousStates}
import ru.itclover.streammachine.aggregators.accums.ContinuousStates.{CountAccumState, NumericAccumState, TruthAccumState}
import ru.itclover.streammachine.core.PhaseParser.Functions._
import scala.concurrent.duration._
import ru.itclover.streammachine.core.Time._
import ru.itclover.streammachine.phases.{ConstantPhases, NoState}
import scala.language.{higherKinds, implicitConversions}
import org.scalatest.prop.PropertyChecks
import ru.itclover.streammachine.utils.ParserMatchers


class AccumsInvariantsTest extends WordSpec with ParserMatchers with PropertyChecks {

  val testResults = Seq(
    (Seq(Success(1.0), Success(2.0), Success(3.0), Success(4.0), Success(5.0))),
    (Seq(Failure("Test"))),
    (Seq(Failure("Test"), Success(1.0))),
    (Seq(Success(1.0), Failure("Test"))),
    (Seq(Failure("Test"), Failure("Test"), Failure("Test"), Failure("Test"), Failure("Test"), Failure("Test"))),
    (Seq())
  )

  type PhaseToDoubleAccum = TestPhase[Double] => AccumPhase[TestingEvent[Double], NoState, Double, Double]
  type PhaseToLongAccum = TestPhase[Double] => AccumPhase[TestingEvent[Double], NoState, Double, Long]
  type BoolPhaseToLongAccum = TestPhase[Boolean] => AccumPhase[TestingEvent[Double], NoState, Double, Long]

  val doublePhases: Seq[(String, PhaseToDoubleAccum, PhaseToDoubleAccum)] = Seq(
    ("avg", avg(_, 2.seconds), avg.continuous(_, 2.seconds)),
    ("sum", sum(_, 2.seconds), sum.continuous(_, 2.seconds))
  )
  val longPhases: Seq[(String, PhaseToLongAccum, PhaseToLongAccum)] = Seq(
    ("count", count(_, 2.seconds), count.continuous(_, 2.seconds)),
    ("millisCount", millisCount(_, 2.seconds), millisCount.continuous(_, 2.seconds))
  )
  // todo
  /*val boolPhases: Seq[(String, BoolPhaseToLongAccum, BoolPhaseToLongAccum)] = Seq(
    ("truthCount", function, truthCount.continuous(_, 2.seconds)),
    ("truthMillisCount", truthMillisCount(_, 2.seconds), truthMillisCount.continuous(_, 2.seconds))
  )*/


  val doublePhasesAndResults = (for (r <- testResults; p <- doublePhases) yield (r, p))
  val longPhasesAndResults = (for (r <- testResults; p <- longPhases) yield (r, p))

  "All combinations of events and phases (Queue, OneTime)" should {
    "return the same result for double accums" in {
      doublePhasesAndResults map {
        case (results: Seq[PhaseResult[Double]], (id: String, p1: PhaseToDoubleAccum, p2: PhaseToDoubleAccum)) =>
          val events = for((t, res) <- times.take(results.length).zip(results)) yield TestingEvent(res, t)
          val oneTime = applyOnTestEvents[Double, Double](p1(_), events)
          val continuous = applyOnTestEvents[Double, Double](p2(_), events)
          (id, oneTime) shouldEqual ((id, continuous))
      }
    }

    "return the same result for long accums" in {
      longPhasesAndResults map {
        case (results: Seq[PhaseResult[Double]], (id: String, p1: PhaseToLongAccum, p2: PhaseToLongAccum)) =>
          val events = for((t, res) <- times.take(results.length).zip(results)) yield TestingEvent(res, t)
          val oneTime = applyOnTestEvents[Double, Long](p1(_), events)
          val continuous = applyOnTestEvents[Double, Long](p2(_), events)
          (id, oneTime) shouldEqual ((id, continuous))
      }
    }
  }
}