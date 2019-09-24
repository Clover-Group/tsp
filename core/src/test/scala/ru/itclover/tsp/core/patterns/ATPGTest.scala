package ru.itclover.tsp.core.patterns

import cats.Id
import org.scalatest.{FlatSpec, Matchers}
import ru.itclover.tsp.core._
import ru.itclover.tsp.core.fixtures.Common._
import ru.itclover.tsp.core.fixtures.Event

import scala.language.reflectiveCalls

class ATPGTest extends FlatSpec with Matchers {

  it should "auto generate all patterns" in {

    val testNum = 5

    // Expected state
    val expState = SimplePState(PQueue.empty)

    // Actual state
    val ev = Event[Int](0L, testNum, 0)
    val pat = ConstPattern[EInt, Int](Result.succ(testNum))(extractor)

    // Assertion
    val actState = StateMachine[Id].run(pat, Seq(ev), pat.initialState())

    actState.queue.size shouldBe 0
  }
}
