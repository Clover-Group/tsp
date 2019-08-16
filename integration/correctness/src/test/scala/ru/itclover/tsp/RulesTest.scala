/*
package ru.itclover.tsp

import java.time.Instant
import org.scalatest.{Matchers, WordSpec}
import ru.itclover.tsp.aggregators.AggregatorPhases.{Derivation, ToSegments}
import ru.itclover.tsp.phases.NumericPhases._
import ru.itclover.tsp.core.{Pattern, PatternResult, Window}
import ru.itclover.tsp.core.PatternResult.{Failure, Success}
import ru.itclover.tsp.core.Pattern.Functions._
import ru.itclover.tsp.phases.Phases.{Decreasing, Increasing}
import ru.itclover.tsp.http.utils.{Timer => TimerGenerator, _}
import ru.itclover.tsp.phases.BooleanPhases.Assert
import scala.concurrent.duration._
import scala.util.Random


case class Row(time: Instant, speed: Double, pump: Double, wagonId: Int = 0)

class TimeSeriesGeneratorTestCase extends WordSpec with Matchers {

  import ru.itclover.tsp.core.Time._
  import Predef.{any2stringadd => _, assert => _, _}

  implicit val random: Random = new java.util.Random(345l)

  implicit val symbolNumberExtractorEvent = new SymbolNumberExtractor[Row] {
    override def extract(event: Row, symbol: Symbol) = {
      symbol match {
        case 'speed => event.speed
        case 'pump => event.pump
        case _ => sys.error(s"No field $symbol in $event")
      }
    }
  }
  implicit val timeExtractor: TimeExtractor[Row] = new TimeExtractor[Row] {
    override def apply(v1: Row) = v1.time
  }

  def fakeMapper[Event, PhaseOut](p: Pattern[Event, _, PhaseOut]) = FakeMapper[Event, PhaseOut]()


  def run[Event, Out](rule: Pattern[Event, _, Out], events: Seq[Event]): Vector[PatternResult.TerminalResult[Out]] = {
    val mapResults = fakeMapper(rule)
    events
      .foldLeft(PatternMapper(rule, mapResults)) { case (machine, event) => machine(event) }
      .result
  }

  def runWithSegmentation[Event, Out](rule: Pattern[Event, _, Out], events: Seq[Event])
                                     (implicit te: TimeExtractor[Event]) = {
    val mapResults = segmentMapper(rule)(te)
    events
      .foldLeft(PatternMapper(rule, mapResults)) { case (machine, event) => machine(event) }
      .result
  }

  type Phase[Row] = Pattern[Row, _, _]

  "Combine And & Assert parsers" should {
    "work correctly" in {
      import ru.itclover.tsp.phases.NumericPhases._
      val phase: Phase[Row] = Assert('speed.asDouble > 10.0) and Assert('speed.asDouble < 20.0)

      val rows = (
        for (time <- TimerGenerator(from = Instant.now());
             speed <- Constant(30.0).timed(1.seconds)
               .after(Change(from = 30.0, to = 0.0, howLong = 10.seconds))
               .after(Constant(0.0))
        ) yield Row(time, speed.toInt, 0)
        ).run(seconds = 10)

      val results = run(phase, rows)

      println(s"Results = $results")

      assert(results.nonEmpty)

      val (success, failures) = results partition {
        case Success(_) => true
        case Failure(_) => false
      }

      success.length should be > 1
      failures.length should be > 1
    }
  }

  "dsl" should {

    "works" in {
      import ru.itclover.tsp.core.Time._

      import Predef.{any2stringadd => _, assert => _, _}

      implicit val random: Random = new java.util.Random(345l)

      val window: Window = 5.seconds

      type Phase[Row] = Pattern[Row, _, _]

      val phase: Phase[Row] = avg((e: Row) => e.speed, 2.seconds) > 100.0

      val rows = (
        for (time <- TimerGenerator(from = Instant.now());
             pump <- RandomInRange(1, 100).map(_.toDouble).timed(40.second)
               .after(Constant(0));
             speed <- Constant(261.0).timed(1.seconds)
               .after(Change(from = 260.0, to = 0.0, howLong = 10.seconds))
               .after(Constant(0.0))
        ) yield Row(time, speed.toInt, pump.toInt)
        ).run(seconds = 10)

      val results = run(phase, rows)

      assert(results.nonEmpty)
    }

  }


  "Result segmentation" should {

    implicit val random: Random = new java.util.Random(345l)

    implicit val symbolNumberExtractorEvent = new SymbolNumberExtractor[Row] {
      override def extract(event: Row, symbol: Symbol) = {
        symbol match {
          case 'speed => event.speed
          case 'pump => event.pump
          case _ => sys.error(s"No field $symbol in $event")
        }
      }
    }

    implicit val timeExtractor = new TimeExtractor[Row] {
      override def apply(v1: Row) = v1.time
    }

    "work on not segmented output" in {
      val phase: Phase[Row] = Assert('speed.asDouble > 35.0)
      val rows = (
        for (time <- TimerGenerator(from = Instant.now());
             speed <- Constant(50.0).timed(1.seconds)
               .after(Change(from = 50.0, to = 30.0, howLong = 20.seconds))
               .after(Constant(0.0))
        ) yield Row(time, speed.toInt, 0)
        ).run(seconds = 20)
      val (successes, failures) = runWithSegmentation(phase, rows).partition(_.isInstanceOf[Success[Segment]])

      failures should not be empty
      successes should not be empty
      successes.length should equal(1)

      val segmentLengthOpt = successes.head match {
        case Success(Segment(from, to)) => Some(to.toMillis - from.toMillis)
        case _ => None
      }
      segmentLengthOpt should not be empty
      segmentLengthOpt.get should be > 12000L
      segmentLengthOpt.get should be < 20000L
    }

    "Segment Increasing" in {
      val phase: Phase[Row] = ToSegments(Increasing(_.speed, 35.0, 50.0))
      val rows = (
        for (time <- TimerGenerator(from = Instant.now());
             speed <- Constant(30.0).timed(2.seconds)
               .after(Change(from = 30.0, to = 35.0, howLong = 5.seconds))
               .after(Constant(35.0).timed(1.seconds))
               .after(Change(from = 35.0, to = 50.0, howLong = 13.seconds))
               .after(Constant(51.0).timed(1.seconds))
        ) yield Row(time, speed.toInt, 0)
          ).run(seconds = 15)

      println(rows.map(_.speed))

      val (successes, failures) = runWithSegmentation(phase, rows).partition(_.isInstanceOf[Success[_]])

      println(successes)

      failures.length should be > 0
      successes should not be empty
      successes.length should equal(1)

      val segmentLengthOpt = successes.head match {
        case Success(Segment(from, to)) => Some(to.toMillis - from.toMillis)
        case _ => None
      }
      segmentLengthOpt should not be empty
      segmentLengthOpt.get should be > 3000L
      segmentLengthOpt.get should be < 10000L
    }
  }

}
 */
