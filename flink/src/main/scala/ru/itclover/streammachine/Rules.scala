package ru.itclover.streammachine

import ru.itclover.streammachine.RulesDemo.Row2
import ru.itclover.streammachine.phases.Phases.{Assert, Decreasing, Timer, Wait}
import scala.language.dynamics

object Rules {


  //    +1_____________Остановка без прокачки масла
  //    1. начало куска ContuctorOilPump не равен 0 И SpeedEngine уменьшается с 260 до 0
  //    2. конец когда SpeedEngine попрежнему 0 и ContactorOilPump = 0 и между двумя этими условиями прошло меньше 60 сек
  //      """ЕСЛИ SpeedEngine перешел из "не 0" в "0" И в течение 90 секунд суммарное время когда ContactorBlockOilPumpKMN = "не 0" менее 60 секунд"""
  val stopWithoutOilPumping =
  ((Assert[Row2](_.contuctorOilPump != 0) & Decreasing(_.speedEngine, 260, 0))
    .andThen(Assert(_.speedEngine == 0)) &
    (Wait[Row2](_.contuctorOilPump == 0) & Timer(_.time, atMaxSeconds = 60)))
    .map {
      case (row, (_, (condition, (start, end)))) => row.wagonId -> s"Result: $start, $end"
    }


//  val myObject = new Myclass
//
//  myObject.anotherParam = 3

}

 class Myclass extends Dynamic{
   val map: Map[String, String] = Map("foo" -> "bar").withDefaultValue("baz")

   def selectDynamic[A](name: String) = map(name).asInstanceOf[A]
 }
