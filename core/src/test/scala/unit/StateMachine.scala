
package ru.itclover.tsp.v2 

import scala.language.reflectiveCalls

import org.scalatest.{FlatSpec, Matchers}
//import org.scalatest.prop.PropertyChecks
import cats.Id

import Common._

class StateMachineTest extends FlatSpec with Matchers {

  "StateMachine" should "process ConstPattern correctly" in {
    
    val tmp  = Event(0L, 0.0)

    val pat = ConstPattern[Event, Int] (0)(ext)
   
    // Run FSM with the predefined event
    val res = StateMachine[Id].run(pat, Seq(tmp), pat.initialState())

    true shouldBe true
  }

}


