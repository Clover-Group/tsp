package ru.itclover.streammachine

import java.time.Instant
import org.scalatest.{Matchers, WordSpec}
import ru.itclover.streammachine.aggregators.AggregatorPhases.{Derivation, ToSegments}
import ru.itclover.streammachine.phases.NumericPhases._
import ru.itclover.streammachine.core.{PhaseParser, PhaseResult, TimeInterval, Window}
import ru.itclover.streammachine.core.PhaseResult.{Failure, Success}
import ru.itclover.streammachine.core.PhaseResult.{Failure, Success}
import ru.itclover.streammachine.core.PhaseParser.Functions._
import ru.itclover.streammachine.phases.Phases.{Decreasing, Increasing}
import ru.itclover.streammachine.http.utils.{Timer => TimerGenerator, _}
import ru.itclover.streammachine.phases.BooleanPhases.Assert
import ru.itclover.streammachine.phases.TimePhases.Wait
import scala.concurrent.duration.Duration
import scala.concurrent.duration._
import scala.util.Random


case class Row(time: Instant, speed: Double, pump: Double, wagonId: Int = 0)

class RulesTest extends WordSpec with Matchers {

  import ru.itclover.streammachine.core.Time._
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

  def fakeMapper[Event, PhaseOut](p: PhaseParser[Event, _, PhaseOut]) = FakeMapper[Event, PhaseOut]()


  def segmentMapper[Event, PhaseOut](p: PhaseParser[Event, _, PhaseOut])(implicit te: TimeExtractor[Event]) =
    SegmentResultsMapper[Event, PhaseOut]()(te)


  def run[Event, Out](rule: PhaseParser[Event, _, Out], events: Seq[Event]): Vector[PhaseResult.TerminalResult[Out]] = {
    val mapResults = fakeMapper(rule)
    events
      .foldLeft(PatternMapper(rule, mapResults)) { case (machine, event) => machine(event) }
      .result
  }

  def runWithSegmentation[Event, Out](rule: PhaseParser[Event, _, Out], events: Seq[Event])
                                     (implicit te: TimeExtractor[Event]) = {
    val mapResults = segmentMapper(rule)(te)
    events
      .foldLeft(PatternMapper(rule, mapResults)) { case (machine, event) => machine(event) }
      .result
  }

  type Phase[Row] = PhaseParser[Row, _, _]

  "Timer phase" should {
    "work correctly" in {
      val speedGte100ForSomeTime = Wait('speed.field >= 90.0).timed(TimeInterval(1.seconds, 2.seconds))

      val rows = (
        for (time <- TimerGenerator(from = Instant.now());
             speed <- Constant(100.0).timed(4.seconds)
               .after(Change(from = 100.0, to = 0.0, howLong = 10.seconds))
               .after(Constant(0.0))
        ) yield Row(time, speed.toInt, 0)
        ).run(seconds = 11)

      println(rows)

      val results = run(speedGte100ForSomeTime, rows)

      assert(results.nonEmpty)

      val (success, failures) = results partition {
        case Success(_) => true
        case Failure(_) => false
      }

      success.length should be > 0
      failures.length should be > 1
    }
  }

  "Combine And & Assert parsers" should {
    "work correctly" in {
      import ru.itclover.streammachine.phases.NumericPhases._
      val phase: Phase[Row] = Assert('speed.field > 10.0) and Assert('speed.field < 20.0)

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


  "stopWithoutOilPumping" should {
    //        +1 Остановка без прокачки масла
    //        1. начало куска ContuctorOilPump не равен 0 И SpeedEngine уменьшается с 260 до 0
    //        2. конец когда SpeedEngine попрежнему 0 и ContactorOilPump = 0 и между двумя этими условиями прошло меньше 60 сек
    //          """ЕСЛИ SpeedEngine перешел из "не 0" в "0" И в течение 90 секунд суммарное время когда ContactorBlockOilPumpKMN = "не 0" менее 60 секунд"""
    "match for valid-1" in {
      val rows = (
        for (time <- TimerGenerator(from = Instant.now());
             pump <- RandomInRange(1, 100).map(_.toDouble).timed(40.second)
               .after(Constant(0));
             speed <- Constant(261.0).timed(1.seconds)
               .after(Change(from = 260.0, to = 0.0, howLong = 10.seconds))
               .after(Constant(0.0))
        ) yield Row(time, speed.toInt, pump.toInt, 1)
        ).run(seconds = 100)

      //      val results: Seq[(Int, String)] = run(Rules.stopWithoutOilPumping, rows).collect { case Success(x) => x }
      //
      //      assert(results.nonEmpty)
    }

    "match for valid-2" in {
      val rows = (
        for (time <- TimerGenerator(from = Instant.now());
             pump <- RandomInRange(1, 100).map(_.toDouble).timed(40.second)
               .after(Constant(0));
             speed <- Change(from = 1.0, to = 261, 15.seconds).timed(1.seconds)
               .after(Change(from = 260.0, to = 0.0, howLong = 10.seconds))
               .after(Constant(0.0))
        ) yield Row(time, speed.toInt, pump.toInt, 1)
        ).run(seconds = 100)

    }

  }

  "dsl" should {

    "works" in {
      import ru.itclover.streammachine.core.Time._

      import Predef.{any2stringadd => _, assert => _, _}

      implicit val random: Random = new java.util.Random(345l)

      val window: Window = 5.seconds

      type Phase[Row] = PhaseParser[Row, _, _]

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
      val phase: Phase[Row] = Assert('speed.field > 35.0)
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

    "Segment Decreasing" in {
      // val phase: Phase[Row] = ToSegments(Decreasing(_.speed, 50.0, 35.0))
      val phase: Phase[Row] = ToSegments(Assert(Derivation('speed.field) <= 0.0) and Wait('speed.field <= 35.0))
      val rows = (
        for (time <- TimerGenerator(from = Instant.now());
             speed <- Constant(50.0).timed(1.seconds)
               .after(Change(from = 50.0, to = 35.0, howLong = 5.seconds))
               .after(Constant(35.0).timed(1.seconds))
               .after(Change(from = 35.0, to = 30.0, howLong = 2.seconds))
               .after(Constant(0.0))
        ) yield Row(time, speed.toInt, 0)
        ).run(seconds = 8)
      println(rows.map(_.speed))
      val (successes, failures) = runWithSegmentation(phase, rows).partition(_.isInstanceOf[Success[_]])

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
