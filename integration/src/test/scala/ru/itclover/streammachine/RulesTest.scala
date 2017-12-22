package ru.itclover.streammachine

import java.time.Instant

import org.scalatest.{Matchers, WordSpec}
import ru.itclover.streammachine.RulesDemo.Row2
import ru.itclover.streammachine.core.Aggregators.{Average, ToSegments}
import ru.itclover.streammachine.core.NumericPhaseParser.field
import ru.itclover.streammachine.core.{AliasedParser, PhaseParser, Window}
import ru.itclover.streammachine.core.PhaseResult.{Failure, Success}
import ru.itclover.streammachine.core.Time.{TimeExtractor, more}
import ru.itclover.streammachine.phases.Phases.{Assert, Wait}
import ru.itclover.streammachine.utils.{Timer => TimerGenerator, _}

import scala.concurrent.duration.Duration
import scala.concurrent.duration._
import scala.util.Random

class RulesTest extends WordSpec with Matchers {

  import Rules._
  import core.Aggregators._
  import core.AggregatingPhaseParser._
  import core.NumericPhaseParser._
  import ru.itclover.streammachine.core.Time._
  import Predef.{any2stringadd => _, assert => _, _}


  def run[T, Out](rule: PhaseParser[T, _, Out], events: Seq[T]) = {
    events
      .foldLeft(StateMachineMapper(rule)) { case (machine, event) => machine(event) }
      .result
  }

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

  type Phase[Row] = PhaseParser[Row, _, _]

  "Combine And & Assert parsers" should {
    "work correctly" in {
      val phase: Phase[Row] = 'speed > 10 and 'speed < 20

      val rows = (
        for (time <- TimerGenerator(from = Instant.now());
             speed <- Constant(30.0).timed(1.seconds)
               .after(Change(from = 30.0, to = 0.0, howLong = 10.seconds))
               .after(Constant(0.0))
        ) yield Row(time, speed.toInt, 0)
        ).run(seconds = 10)

      val results = run(phase, rows)

      assert(results.nonEmpty)

      val (success, failures) = results partition {
        case Success(_, _) => true
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

    implicit val random: Random = new java.util.Random(345l)

    "match for valid-1" in {
      val rows = (
        for (time <- TimerGenerator(from = Instant.now());
             pump <- RandomInRange(1, 100).map(_.toDouble).timed(40.second)
               .after(Constant(0));
             speed <- Constant(261.0).timed(1.seconds)
               .after(Change(from = 260.0, to = 0.0, howLong = 10.seconds))
               .after(Constant(0.0))
        ) yield Row2(time, speed.toInt, pump.toInt, 1)
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
        ) yield Row2(time, speed.toInt, pump.toInt, 1)
        ).run(seconds = 100)

      //      val results: Seq[(Int, String)] = run(Rules.stopWithoutOilPumping, rows).collect { case Success(x) => x }
      //
      //      assert(results.nonEmpty)
    }

    //    "not to match" in {
    //      val rows = (
    //        for (time <- TimerGenerator(from = Instant.now());
    //             pump <- RandomInRange(1, 100).map(_.toDouble).timed(40.second)
    //               .after(Constant(0));
    //             speed <- Constant(250d).timed(1.seconds)
    //               .after(Change(from = 250.0, to = 0.0, howLong = 10.seconds))
    //               .after(Constant(0.0))
    //        ) yield Row2(time, speed.toInt, pump.toInt, 1)
    //        ).run(seconds = 100)
    //
    //      val results: Seq[(Int, String)] = run(Rules.stopWithoutOilPumping, rows).collect { case Success(x) => x }
    //
    //      assert(results.isEmpty)
    //    }

  }

  "dsl" should {

    "works" in {
      import core.Aggregators._
      import core.AggregatingPhaseParser._
      import core.NumericPhaseParser._
      import ru.itclover.streammachine.core.Time._

      import Predef.{any2stringadd => _, assert => _, _}

      implicit val random: Random = new java.util.Random(345l)

      val window: Window = 5.seconds

      type Phase[Row] = PhaseParser[Row, _, _]

      val phase: Phase[Row] = avg((e: Row) => e.speed, 2.seconds) > 100

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
      //
      //    val phase2: Phase[Row] = avg('speed, window) > avg('pump, window)
      //
      //    val phase3 = (avg('speed, 5.seconds) >= 5.0) andThen avg('pump, 3.seconds) > 0
      //
      //    val phase4: Phase[Row] = avg((e: Row) => e.speed, 5.seconds) >= value(5.0)
      //
      //    val phase5: Phase[Row] = ('speed > 4 & 'pump > 100).timed(more(10.seconds))
    }

  }

  // todo to PhaseTests
  "Aliased parser" should {
    "Work on different levels of phases trees" in {
      val phase: Phase[Row] = ('speed > 10 and ('speed < 20 as 'upperLimit)) as 'isSpeedInRange

      val rows = (
        for (time <- TimerGenerator(from = Instant.now());
             speed <- Constant(30.0).timed(1.seconds)
               .after(Change(from = 30.0, to = 0.0, howLong = 10.seconds))
               .after(Constant(0.0))
        ) yield Row(time, speed.toInt, 0)
        ).run(seconds = 10)

      val results = run(phase, rows)

      val upperLimitsOpts = results map { result =>
        result.getValue('upperLimit)
      }

      val inRangeOpts = results map { result =>
        result.getValue('isSpeedInRange)
      }

      upperLimitsOpts should contain atLeastOneElementOf None :: Nil
      upperLimitsOpts should contain atLeastOneElementOf Some(true) :: Nil

      inRangeOpts should contain atLeastOneElementOf None :: Nil
      inRangeOpts should contain atLeastOneElementOf Some(true, true) :: Nil
    }
  }

  "ToSegments aggregator" should {

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

    "work on correct input" in {
      val phase: Phase[Row] = ToSegments('speed > 35) // ... Go through again

      val rows = (
        for (time <- TimerGenerator(from = Instant.now());
             speed <- Constant(50.0).timed(1.seconds)
               .after(Change(from = 50.0, to = 30.0, howLong = 20.seconds))
               .after(Constant(0.0))
        ) yield Row(time, speed.toInt, 0)
        ).run(seconds = 20)

      val results = run(phase, rows)

      println(rows)
      println(results)

      println(results.map(x => (x.map(_.asInstanceOf[Segment].from.toMillis), x.map(_.asInstanceOf[Segment].to.toMillis)))
        .zip(rows.map(r => s" is ${r.speed}")))

      assert(results.nonEmpty)
    }
  }

  case class Row(time: Instant, speed: Double, pump: Double)

}


object RulesTest extends App {


  //  (1 to 100).map(_.seconds).map(generator).foreach(println)

}