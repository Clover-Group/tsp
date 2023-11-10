package ru.itclover.tsp.core.patterns

import cats.Id
import org.scalatest.flatspec._

import org.scalatest.matchers.should._
import ru.itclover.tsp.core._
import ru.itclover.tsp.core.fixtures.Common._
import ru.itclover.tsp.core.fixtures.Event

class ATPGTest extends AnyFlatSpec with Matchers {

  it should "auto generate all patterns" in {

    val testNum = 5

    // Actual state
    val ev = Event[Int](0L, 0, testNum, 0)
    val pat = ConstPattern[EInt, Int](Result.succ(testNum))

    // Assertion
    StateMachine[Id].run(pat, Seq(ev), pat.initialState())
// todo check something
  }

}
