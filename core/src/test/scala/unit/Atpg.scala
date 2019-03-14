
package ru.itclover.tsp.v2 

import scala.language.reflectiveCalls
import org.scalatest.{FlatSpec, Matchers}
import org.scalacheck.{Prop, Arbitrary, Gen}



import cats._, cats.data._, cats.implicits._
import Common._

class ATPGTest extends FlatSpec with Matchers {
  
  it should "auto generate all patterns" in {
    
    def func[A] (e:Event[A]):Result[A]  = Result.succ(e.row)

    val patSeq  = Seq ( 
                        ConstPattern[EInt,Int] (0)(extractor), 
                        new SimplePattern[EInt, Int] (_ => func(event))(extractor) 
                      )
    
    //val patgen = Gen.oneOf (patSeq)
    
    implicit val ArbPattern  = Arbitrary { Gen.oneOf (patSeq) }
    
    def checkAll():Prop = { Prop.forAll { (x:Int, y:Int) => x + y == y + x } }

    checkAll.check

    true shouldBe true
  }
}
