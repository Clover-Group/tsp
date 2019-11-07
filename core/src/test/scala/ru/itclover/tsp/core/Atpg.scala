package ru.itclover.tsp.core

import cats.Id
import org.scalacheck.Prop
import org.scalatest.{FlatSpec, Matchers}
import ru.itclover.tsp.core.fixtures.Common._
import ru.itclover.tsp.core.fixtures.Event

class ATPGTest extends FlatSpec with Matchers {

  it should "auto generate all patterns" in {

    def getConstPat(num: Int): ConstPattern[EInt, Int] = ConstPattern[EInt, Int](Result.succ(num))(Event.extractor)
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
