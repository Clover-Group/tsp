package ru.itclover.tsp.core.patterns

import cats.Id
import org.scalatest.{FlatSpec, Matchers}
import ru.itclover.tsp.core._
import ru.itclover.tsp.core.fixtures.Common._
import ru.itclover.tsp.core.fixtures.Event

class ATPGTest extends FlatSpec with Matchers {

  it should "auto generate all patterns" in {

    val testNum = 5

    // Actual state
    val ev = Event[Int](0L, 0, testNum, 0)
    val pat = ConstPattern[EInt, Int](Result.succ(testNum))

    // Assertion
    val actState = StateMachine[Id].run(pat, Seq(ev), pat.initialState())

    actState.queue.size shouldBe 0
  }
}
