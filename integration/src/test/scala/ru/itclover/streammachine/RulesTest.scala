package ru.itclover.streammachine

import java.time.Instant

import org.scalatest.{Matchers, WordSpec}
import ru.itclover.streammachine.RulesDemo.Row2
import ru.itclover.streammachine.core.Aggregators.Average
import ru.itclover.streammachine.core.{PhaseParser, Window}
import ru.itclover.streammachine.core.PhaseResult.Success
import ru.itclover.streammachine.core.Time.{TimeExtractor, more}
import ru.itclover.streammachine.phases.Phases.{Assert, Wait}
import ru.itclover.streammachine.utils.{Timer => TimerGenerator, _}

import scala.concurrent.duration.Duration
import scala.concurrent.duration._
import scala.util.Random

class RulesTest extends WordSpec with Matchers {

  import Rules._

  def run[T, Out](rule: PhaseParser[T, _, Out], events: Seq[T]) = {
    events
      .foldLeft(StateMachineMapper(rule)) { case (machine, event) => machine(event) }
      .result
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

  "customTest" should {

    //    implicit val random: Random = new java.util.Random(345l)
    //
    //    "match" in {
    //      val rows = (
    //        for (time <- TimerGenerator(from = Instant.now());
    //             pump <- RandomInRange(1, 100).map(_.toDouble).timed(40.second)
    //               .after(Constant(0));
    //             speed <- Constant(250d).timed(1.seconds)
    //               .after(Change(from = 250.0, to = 0.0, howLong = 30.seconds))
    //               .after(Constant(0.0))
    //        ) yield Row2(time, speed.toInt, pump.toInt, 1))
    //        .run(seconds = 100)
    //
    //      val rule = Assert[Row2](_.speedEngine > 240) andThen (Wait[Row2](_.contuctorOilPump == 0) & Timer[Row2](_.time, atLeastSeconds = 5))
    //
    //      val results = run(rule, rows).collect { case Success(x) => x }
    //
    //      assert(results.nonEmpty)
    //    }
    //
    //    "works with FlatMap + Average" in {
    //      case class Temp(timeMilliseconds: Long, value: Int)
    //      val rule: Phase[Temp] =
    //        Average(_.timeMilliseconds, _.value, 5.seconds)
    //          .flatMap(avg => Assert[Temp](_.value > avg + 10))
    //          .map { case (event, b) => (event.timeMilliseconds, b) }
    //
    //      for (avg <- Average[Temp, Long, Duration, Int](_.timeMilliseconds, _.value, 5.seconds);
    //           result <- Assert[Temp](_.value > avg + 10)
    //      ) yield {
    //        (result, avg)
    //      }
    //
    //
    //      val points = (
    //        for (time <- Milliseconds;
    //             value <- Constant(0).timed(10.second).after(Constant(100))
    //        ) yield Temp(time, value.toInt)
    //        )
    //        .run(seconds = 100)
    //
    //      val result = run(rule, points).filter(_.isInstanceOf[Success[_]])
    //
    //      assert(result.nonEmpty)
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

      val window: Window = 5.seconds

      type Phase[Row] = PhaseParser[Row, _, _]

      val phase: Phase[Row] = ((e: Row) => e.speed) > 100

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

      println(results)

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

  case class Row(time: Instant, speed: Double, pump: Double)

}


object RulesTest extends App {


  //  (1 to 100).map(_.seconds).map(generator).foreach(println)

}