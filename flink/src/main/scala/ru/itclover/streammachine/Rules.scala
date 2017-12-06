package ru.itclover.streammachine

//import java.time.Instant
//
//import ru.itclover.streammachine.RulesDemo.Row2
//import ru.itclover.streammachine.core.Aggregators.Timer
//import ru.itclover.streammachine.core.{AndThenParser, PhaseParser}
//import ru.itclover.streammachine.phases.Phases._
//import ru.itclover.streammachine.core.TimeImplicits._
//
//import scala.language.dynamics

object Rules {
//
//
//  //    +1_____________Остановка без прокачки масла
//  //    1. начало куска ContuctorOilPump не равен 0 И SpeedEngine уменьшается с 260 до 0
//  //    2. конец когда SpeedEngine попрежнему 0 и ContactorOilPump = 0 и между двумя этими условиями прошло меньше 60 сек
//  //      """ЕСЛИ SpeedEngine перешел из "не 0" в "0" И в течение 90 секунд суммарное время когда ContactorBlockOilPumpKMN = "не 0" менее 60 секунд"""
//  val stopWithoutOilPumping =
//  (Assert[Row2](_.contuctorOilPump != 0) & Decreasing(_.speedEngine, 260, 0))
//    .andThen(Assert[Row2](_.speedEngine == 0) & Wait[Row2](_.contuctorOilPump == 0) & Timer(_.time, atMaxSeconds = 60))
//    .mapWithEvent {
//      case (row, (_, (condition, (start, end)))) => row.wagonId -> s"Result: $start, $end"
//    }
//
//
//  //  ++6_______________Повторный запуск дизеля через малый промежуток времени
//  //  Если CurrentStarterGenerator принял значение 100 меньше, чем за 180 секунд после того,
//  //  как CurrentStarterGenerator стал равен 0 (перешел из не0 в 0
//
//  case class Generator(value: Int, time: Instant)
//
//  val repeatedDieselStart =
//    Decreasing[Generator, Int](_.value, 1, 0) andThen (Wait[Generator](_.value > 100)
//      & Timer(_.time, atMaxSeconds = 100))
//
//  //  +?+7_______________Низкая производительность тормозного компрессора
//  //    CurrentCompressorMotor > 0 И PAirMainRes возрастает с 7,5 до 8 больше чем за 23 секунды
//
//  case class Compressor(currentCompressorMotor: Double, pAirMainRes: Double, time: Instant)
//
//  type Phase[Event] = PhaseParser[Event, _, _]
//
//  val lowPerformanceOfCompressor: Phase[Compressor] = Assert[Compressor](_.currentCompressorMotor > 0) &
//    (Timer[Compressor,Instant](_.time, atLeastSeconds = 23) & Increasing(_.pAirMainRes, 7.5, 8.0))
//


}
