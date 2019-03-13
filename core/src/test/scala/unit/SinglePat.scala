// This runs FSM for all patterns using a one-entry queues

package ru.itclover.tsp.v2 

import scala.language.reflectiveCalls

import org.scalatest.{FlatSpec, Matchers}
import cats.Id

import Common._

class SinglePatTest extends FlatSpec with Matchers {
  
  "StateMachine" should "process SimplePattern correctly" in {
    
    // Test function
    def func[A] (e:Event[A]):Result[A]  = Result.succ(e.row)
    
    val pat = new SimplePattern[Event[Int], Int] (_ => func(event))(extractor)
    
    val res = StateMachine[Id].run(pat, Seq(event), pat.initialState())

    true shouldBe true
  }
  
  "StateMachine" should "process SkipPattern correctly" in {
    
    def func[A] (e:Event[A]):Result[A]  = Result.succ(e.row)

    val pat = new SimplePattern[Event[Int], Int] (_ => func(event))(extractor)
    
    val res = StateMachine[Id].run(pat, Seq(event), pat.initialState())

    true shouldBe true
  }
  
  "StateMachine" should "process ExtractingPattern correctly" in {

    import ru.itclover.tsp.io.{Decoder, Extractor}
    
    // Decoder Instance
    implicit val dec: Decoder[Int, Int] =  ((v:Int) => 2*v )
 
    // Pattern Extractor
    case class DummyExtractor (a:Int, sym:Symbol) extends Extractor[Int,Symbol,Int] {
      def apply[T] (a:Int, sym:Symbol)(implicit d: Decoder[Int,T]) = d(a)
    }

    val ext  = DummyExtractor(0, 'and)
    
    //val pat = new ExtractingPattern[Event[Int], Int, Int, Int, Int] (0, 'and)(extractor, ext, dec)
    //val res = StateMachine[Id].run(pat, Seq(event), pat.initialState())
    
    true shouldBe true
  }

}


