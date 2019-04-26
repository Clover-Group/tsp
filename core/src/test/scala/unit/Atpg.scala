
package ru.itclover.tsp.core

import scala.language.reflectiveCalls
import org.scalatest.{FlatSpec, Matchers}
import org.scalacheck.{Prop, Arbitrary, Gen}

import cats.Id

import Common._

class ATPGTest extends FlatSpec with Matchers {
  
  it should "auto generate all patterns" in {
    
    def getConstPat(num:Int):ConstPattern[EInt,Int] = ConstPattern[EInt,Int] (Result.succ(num))(extractor)

    // Checker property
    def checkAll():Prop= { 

      Prop.forAll { num:Int =>  

        // Exp state
        val eventsQueue  = PQueue(IdxValue(num,Result.succ(0)))
        val expState  = SimplePState[Int] (eventsQueue)

        // Act state
        val ev = Event[Int](0L, num, 0)
        val pat = getConstPat (num)
        
        // Assertion
        val actState = StateMachine[Id].run(pat, Seq(ev), pat.initialState())
        actState === expState
      }
    }

    checkAll.check

    true shouldBe true
  }
}
