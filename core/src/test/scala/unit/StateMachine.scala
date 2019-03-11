
package ru.itclover.tsp.v2 


import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.prop.PropertyChecks
import cats.Id

import ru.itclover.tsp.v2.{Patterns, StateMachine}

class StateMachineTest extends FlatSpec with Matchers {
  "StateMachine" should "Init correctly" in {
    

    val pat = ConstPattern
    val res = StateMachine[Id].run(pat, SimplePState, pat.initialState())

    true shouldBe true
  }

}


