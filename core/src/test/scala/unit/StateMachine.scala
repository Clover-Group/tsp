
package ru.itclover.tsp.v2 

import scala.language.reflectiveCalls

import org.scalatest.{FlatSpec, Matchers}
//import org.scalatest.prop.PropertyChecks
import cats.Id

import Common._

class StateMachineTest extends FlatSpec with Matchers {
  
  val event = Event[Int](0L, 0, 0)

  "StateMachine" should "process ConstPattern correctly" in {
    
    val pat = ConstPattern[Event[Int], Int] (0)(extractor)
    val res = StateMachine[Id].run(pat, Seq(event), pat.initialState())

    true shouldBe true
  }
  
  "StateMachine" should "process SimplePattern correctly" in {
    
    // Test function
    def func[A] (e:Event[A]):Result[A]  = Result.succ(e.row)
    
    val pat = new SimplePattern[Event[Int], Int] (_ => func(event))(extractor)
    
    val res = StateMachine[Id].run(pat, Seq(event), pat.initialState())

    true shouldBe true
  }

}


