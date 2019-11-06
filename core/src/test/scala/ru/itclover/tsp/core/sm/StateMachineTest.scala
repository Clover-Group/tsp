// This instantiates a Core FSM and shows how to it processes an event queue
// FSM operates 100% like a State Monad and should be rewritten as Cats/ScalaZ State Monad instead of a custom code

package ru.itclover.tsp.core.sm

import cats._
import org.scalatest.{FlatSpec, Matchers}
import ru.itclover.tsp.core._
import ru.itclover.tsp.core.fixtures.Common._

import scala.collection.mutable.ArrayBuffer

class StateMachineTest extends FlatSpec with Matchers {

  it should "process ConstPattern correctly" in {

    // Run FSM
    val pat = ConstPattern[EInt, Int](Result.succ(0))
    val collect = new ArrayBuffer[IdxValue[Int]]()
    StateMachine[Id].run(pat, Seq(event), pat.initialState(), (x: IdxValue[Int]) => collect += x)

    // Instantiate an output state manually
    val eventsQueue = PQueue(IdxValue(1, 1, Result.succ(0)))

    // Temporary, until custom Equality[IdxValue] is implemented
    eventsQueue.size shouldBe collect.size
    eventsQueue.toSeq.zip(collect).forall(ab => ab._1 == ab._2) shouldBe true
  }

}
