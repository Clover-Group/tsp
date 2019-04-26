// This instantiates a Core FSM and shows how to it processes an event queue
// FSM operates 100% like a State Monad and should be rewritten as Cats/ScalaZ State Monad instead of a custom code

package ru.itclover.tsp.v2

import cats._
import org.scalatest.{FlatSpec, Matchers}
import ru.itclover.tsp.v2.Common._

import scala.collection.mutable.ArrayBuffer
import scala.language.reflectiveCalls

class StateMachineTest extends FlatSpec with Matchers {

  it should "process ConstPattern correctly" in {

    // Run FSM
    val pat = ConstPattern[EInt, Int](Result.succ(0))(extractor)
    val collect = new ArrayBuffer[IdxValue[Int]]()
    val actState = StateMachine[Id].run(pat, Seq(event), pat.initialState(), (x: IdxValue[Int]) => collect += x)

    // Instantiate an output state manually
    val eventsQueue = PQueue(IdxValue(1, Result.succ(0)))
    val expState = SimplePState[Int](eventsQueue)

    // Temporary, until custom Equality[IdxValue] is implemented
    expState.queue.size shouldBe collect.size
  }

}
