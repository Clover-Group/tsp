// This instantiates a Core FSM and shows how to it processes an event queue
// FSM operates 100% like a State Monad and should be rewritten as Cats/ScalaZ State Monad instead of a custom code

package ru.itclover.tsp.v2 

import scala.language.reflectiveCalls
import org.scalatest.{FlatSpec, Matchers}
import cats._
import cats.implicits._

import Common._

class StateMachineTest extends FlatSpec with Matchers {
  
  it should "process ConstPattern correctly" in {
    
    // Run FSM
    val pat   = ConstPattern[EInt, Int] (0)(extractor)
    val actState = StateMachine[Id].run(pat, Seq(event), pat.initialState())
    
    // Instantiate an output state manually
    val eventsQueue  = scala.collection.mutable.Queue(IdxValue(1,Result.succ(0)))
    val expState  = SimplePState[Int] (eventsQueue)

    expState shouldBe actState
  }

}
