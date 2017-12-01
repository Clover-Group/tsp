package ru.itclover.streammachine.core.v2

import java.time.Instant
import java.time.temporal.{ChronoUnit, TemporalUnit}

import ru.itclover.streammachine.core.{PhaseParser, PhaseResult}

import scala.concurrent.duration.Duration
import scala.concurrent.duration._

class EventPattern {

  // trigger1 ~ seconds{,10}? ~ trigger2

  trait DurationLimit{
    def isGood(eventTime: Instant): Boolean
  }

  case class SoonerThan(duration:Duration) extends DurationLimit{
    override def isGood(eventTime: Instant): Boolean = eventTime.plus(duration.toMillis,ChronoUnit.MILLIS).isBefore(eventTime)
  }


//  case class TimeRanges[Event, Out](durationLimit: DurationLimit, ) extends PhaseParser[Event, Seq[(Instant, Instant)], Out] {
//
//    override def apply(event: Event, v2: Seq[(Instant, Instant)]): (PhaseResult[Out], Seq[(Instant, Instant)]) = {
//
//    }
//
//    override def initialState: Seq[(Instant, Instant)] = Seq.empty
//  }


  trait Pattern

  case class Condition[T](predicate: T => Boolean) extends Pattern




  abstract class Timed[T](starts: Seq[(Instant, Instant)], duration: Duration) extends Pattern {

    def apply(f: T => Instant) = {

    }

  }

  // CurrentCompressorMotor > 0 И PAirMainRes возрастает с 7,5 до 8 больше чем за 23 секунды

  // Assert(_.mainRes < 7.5)

  //  (B_SINHR_1 = 0 И B_SKOL = 0 И B_SINHR_2 = 0) И inAwtoPesok = 0 И (alfar_vip2 И alfar_vip1) не уменьшается более 4 секунд

  //  condition(B_SINHR_1 = 0 И B_SKOL = 0 И B_SINHR_2 = 0) И inAwtoPesok = 0 ) & (Growing(alfar_vip2) & Growing(alfar_vip1)).moreThan (4 sec)


  //  ++6_______________Повторный запуск диезля через малый промежуток времени
  //  Если CurrentStarterGenerator принял значение 100 меньше, чем за 180 секунд после того,
  //  как CurrentStarterGenerator стал равен 0 (перешел из не0 в

  // Case class @#ADFASDF(Current:Int, )
  // $ => x;
  // x => x.currenStatGen != 0
  // condition($.currentStarGen != 0) ~ condition(f == 0) ~ gap(< 100 seconds) ~ condition(f >= 100)

  //  ++5_______________Нарушение условий движения локомотива
  //    ЕСЛИ (PosKM = "не 0" И SpeedEngine >350 И PowerPolling > 100 И WaterRheostat = 0 И Speed = 0 И
  //      (ReversLockForward =не0 ИЛИ ReversLockBack = не0)) в течение > 10 сек
  //
  // condition(f != 0 ...).{, 10seconds}?


  //  +?+7_______________Низкая производительность тормозного компрессора
  //    CurrentCompressorMotor > 0 И PAirMainRes возрастает с 7,5 до 8 больше чем за 23 секунды
  // pattern =  changed(from, to) ~ (increasing(field) ~ gap(> 23)) ~ condition(>=8)
  //  condition(currentCompressorMotor > 0) & ( pattern)


  // параметр переключился 3 раза за <30 секунд
  // ((changed(f) ~ gap(< 30 seconds) ~ changed(f) ~ gap(< 30 seconds) ~ changed(f))).lessThan(3sec)

  case class GapLess(duration: Duration, left: Pattern, right: Pattern)



}
