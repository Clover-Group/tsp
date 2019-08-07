// This runs FSM for all patterns using a one-entry queues

package ru.itclover.tsp.core

import cats.Id
import org.scalatest.{FlatSpec, Matchers}
import ru.itclover.tsp.core.Common._
import ru.itclover.tsp.core.io.{Decoder, Extractor}

import scala.language.reflectiveCalls

class SinglePatTest extends FlatSpec with Matchers {

  it should "process SimplePattern correctly" in {

    // Test function
    def func[A](e: Event[A]): Result[A] = Result.succ(e.row)

    val pat = new SimplePattern[EInt, Int](_ => func(event))(extractor)

    val res = StateMachine[Id].run(pat, Seq(event), pat.initialState())

    true shouldBe true
  }

  it should "process SkipPattern correctly" in {

    def func[A](e: Event[A]): Result[A] = Result.succ(e.row)

    val pat = new SimplePattern[EInt, Int](_ => func(event))(extractor)

    val res = StateMachine[Id].run(pat, Seq(event), pat.initialState())

    true shouldBe true
  }

  it should "process ExtractingPattern correctly" in {

    // Decoder Instance
    implicit val dec: Decoder[Int, Int] = ((v: Int) => 2 * v)

    // Pattern Extractor

    implicit val MyExtractor = new Extractor[EInt, Symbol, Int] {
      def apply[T](a: EInt, sym: Symbol)(implicit d: Decoder[Int, T]): T = a.row
    }

    //val pat = new ExtractingPattern[EInt, Symbol, Int, Int, Int] ('and, 'or)(extractor, MyExtractor, dec)
    val pat = new ExtractingPattern('and, 'or)(extractor, MyExtractor, dec)

    val res = StateMachine[Id].run(pat, Seq(event), pat.initialState())

    true shouldBe true
  }

}
