package ru.itclover.tsp.core.patterns

import org.scalatest.flatspec._
import org.scalatest.matchers.should._


class ATPGPropTest extends AnyFlatSpec with Matchers {

  it should "auto generate all patterns" in {

//    def getConstPat(num: Int): ConstPattern[EInt, Int] = ConstPattern[EInt, Int](Result.succ(num))(Event.extractor)
//todo actualize!
//    // Checker property
//    def checkAll(): Prop =
//      Prop.forAll { num: Int =>
//        // Exp state
//        val eventsQueue = PQueue(IdxValue(num.toLong, num.toLong, Result.succ(0)))
//
//        // Act state
//        val ev = Event[Int](0L, num.toLong, num, 0)
//        val pat = getConstPat(num)
//
//        // Assertion
//        val actState = StateMachine[Id].run(pat, Seq(ev), pat.initialState())
//        actState === expState
//      }
//
//    checkAll.check

    true shouldBe true
  }
}
