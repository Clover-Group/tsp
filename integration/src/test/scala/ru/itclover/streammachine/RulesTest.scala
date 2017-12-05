package ru.itclover.streammachine

import java.time.Instant

import org.scalatest.{Matchers, WordSpec}
import ru.itclover.streammachine.Rules.Phase
import ru.itclover.streammachine.RulesDemo.Row2
import ru.itclover.streammachine.core.Aggregators.Average
import ru.itclover.streammachine.core.PhaseParser
import ru.itclover.streammachine.core.PhaseResult.Success
import ru.itclover.streammachine.phases.Phases.{Assert, Timer, Wait}
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

      val results: Seq[(Int, String)] = run(Rules.stopWithoutOilPumping, rows).collect { case Success(x) => x }

      assert(results.nonEmpty)
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

      val results: Seq[(Int, String)] = run(Rules.stopWithoutOilPumping, rows).collect { case Success(x) => x }

      assert(results.nonEmpty)
    }

    "not to match" in {
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
    }

  }

  "customTest" should {

    implicit val random: Random = new java.util.Random(345l)

    "match" in {
      val rows = (
        for (time <- TimerGenerator(from = Instant.now());
             pump <- RandomInRange(1, 100).map(_.toDouble).timed(40.second)
               .after(Constant(0));
             speed <- Constant(250d).timed(1.seconds)
               .after(Change(from = 250.0, to = 0.0, howLong = 30.seconds))
               .after(Constant(0.0))
        ) yield Row2(time, speed.toInt, pump.toInt, 1))
        .run(seconds = 100)

      val rule = Assert[Row2](_.speedEngine > 240) andThen (Wait[Row2](_.contuctorOilPump == 0) & Timer[Row2](_.time, atLeastSeconds = 5))

      val results = run(rule, rows).collect { case Success(x) => x }

      assert(results.nonEmpty)
    }

    "works with FlatMap + Average" in {
      import ru.itclover.streammachine.core.TimeImplicits._
      case class Temp(timeMilliseconds: Long, value: Int)
      val rule: Phase[Temp] =
        Average[Temp, Long, Duration, Int](_.timeMilliseconds, _.value, 5.seconds)
          .flatMap(avg => Assert[Temp](_.value > avg + 10))
          .map { case (event, b) => (event.timeMilliseconds, b) }

      for (avg <- Average[Temp, Long, Duration, Int](_.timeMilliseconds, _.value, 5.seconds);
           result <- Assert[Temp](_.value > avg + 10)
      ) yield {
        (result, avg)
      }


      val points = (
        for (time <- Milliseconds;
             value <- Constant(0).timed(10.second).after(Constant(100))
        ) yield Temp(time, value.toInt)
        )
        .run(seconds = 100)

      val result = run(rule, points).filter(_.isInstanceOf[Success[_]])

      assert(result.nonEmpty)
    }
  }


}


object RulesTest extends App {

  case class Row(time: Instant, speed: Double, pump: Double)


  //  (1 to 100).map(_.seconds).map(generator).foreach(println)

}