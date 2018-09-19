package ru.itclover.tsp.core

import org.scalatest.{Matchers, WordSpec}
import ru.itclover.tsp.aggregators.AggregatorPhases.{Aligned, Skip}
import ru.itclover.tsp.core.PatternResult.{Failure, Stay, Success, TerminalResult}
import ru.itclover.tsp.aggregators.AggregatorPhases._
import ru.itclover.tsp.aggregators.accums.{AccumPhase, OneTimeStates, ContinuousStates}
import ru.itclover.tsp.aggregators.accums.ContinuousStates.{CountAccumState, NumericAccumState, TruthAccumState}
import ru.itclover.tsp.core.Pattern.Functions._
import scala.concurrent.duration._
import ru.itclover.tsp.core.Time._
import ru.itclover.tsp.phases.{ConstantPhases, NoState}
import scala.language.{higherKinds, implicitConversions}
import org.scalatest.prop.PropertyChecks
import ru.itclover.tsp.utils.ParserMatchers


// TODO fix divergence in oneTimers, or rm tests
// tests not working for now - small divergence due to situations when even oneTime accums need to
// pop out older elements, for example
// (sum(1.0, 1.0, Stay, 1.0), 2.sec) -> 3.0, but
// (sum.continuous(1.0, 1.0, Stay, 1.0), 2.sec) -> 2.0 (drop first 1.0 on last event)
class AccumsInvariantsTest extends WordSpec with ParserMatchers with PropertyChecks {

  val testResults = Seq(
    (for (i <- 1 to 10000) yield {
      if (i % 100 == 0) Failure("Test") else if (i % 10 == 0) Stay else Success(Math.random() * 100.0)
    }),
    (Seq(Success(1.0), Success(2.0), Stay, Success(3.0), Success(4.0), Failure("Test"), Success(5.0))),
    (Seq(Success(1.0), Success(2.0), Success(3.0), Success(4.0), Success(5.0))),
    (Seq(Failure("Test"))),
    (Seq(Failure("Test"), Success(1.0))),
    (Seq(Success(1.0), Failure("Test"))),
    (Seq(Failure("Test"), Failure("Test"), Failure("Test"), Failure("Test"), Failure("Test"), Failure("Test"))),
    (Seq())
  )

  type PhaseToDoubleAccum = TestPhase[Double] => Pattern[TestEvent[Double], _, Double]
  type PhaseToLongAccum = TestPhase[Double] => AccumPhase[TestEvent[Double], NoState, Double, Long]
  type BoolPhaseToLongAccum = TestPhase[Boolean] => AccumPhase[TestEvent[Double], NoState, Double, Long]

  val doublePhases: Seq[(String, PhaseToDoubleAccum, PhaseToDoubleAccum)] = Seq(
    ("avg", avg(_, 2.seconds), avg.continuous(_, 2.seconds)),
    ("sum", sum(_, 2.seconds), sum.continuous(_, 2.seconds)),
    /*("\nsum andThen sum",
      (p: TestPhase[Double]) => sum(p, 4.seconds) minus Aligned(2.seconds, sum(p, 2.seconds)),
      (p: TestPhase[Double]) => sum(p, 4.seconds) minus sum.continuous(p, 2.seconds)),

    ("sum and aligned(sum)",
      (p: TestPhase[Double]) => (sum(p, 2.seconds) plus Aligned(2.seconds, sum(p, 3.seconds))),
      (p: TestPhase[Double]) => sum.continuous(p, 4.seconds)),*/
    ("\nsum(sum)",
      (p: TestPhase[Double]) => sum(sum.continuous(p, 2.seconds), 2.seconds),
      (p: TestPhase[Double]) => sum.continuous(sum.continuous(p, 2.seconds), 2.seconds))
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

//  "All combinations of events and phases (Queue, OneTime)" should {
//    "return the same result for double accums" in {
//      doublePhasesAndResults map {
//        case (results: Seq[PatternResult[Double]], (id: String, p1: PhaseToDoubleAccum, p2: PhaseToDoubleAccum)) =>
//          val events = for((t, res) <- times.take(results.length).zip(results)) yield TestEvent(res, t)
//          val oneTime = applyOnTestEvents[Double, Double](p1(_), events)
//          val continuous = applyOnTestEvents[Double, Double](p2(_), events)
//          println(s"Tested for testResults = `$testResults`")
//          (id, oneTime) shouldEqual ((id, continuous))
//      }
//    }
//
//    "return the same result for long accums" in {
//      longPhasesAndResults map {
//        case (results: Seq[PatternResult[Double]], (id: String, p1: PhaseToLongAccum, p2: PhaseToLongAccum)) =>
//          val events = for((t, res) <- times.take(results.length).zip(results)) yield TestEvent(res, t)
//          val oneTime = applyOnTestEvents[Double, Long](p1(_), events)
//          val continuous = applyOnTestEvents[Double, Long](p2(_), events)
//          (id, oneTime) shouldEqual ((id, continuous))
//      }
//    }
//  }
}