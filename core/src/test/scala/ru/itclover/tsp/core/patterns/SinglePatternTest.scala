// This runs FSM for all patterns using a one-entry queues

package ru.itclover.tsp.core.patterns

import cats.Id
import org.scalatest.{FlatSpec, Matchers}
import ru.itclover.tsp.core._
import ru.itclover.tsp.core.fixtures.Common._
import ru.itclover.tsp.core.fixtures.Event
import ru.itclover.tsp.core.io.{Decoder, Extractor}

class SinglePatternTest extends FlatSpec with Matchers {

  def processEvent[A](e: Event[A]): Result[A] = Result.succ(e.row)

  it should "process SimplePattern correctly" in {

    val pat = new SimplePattern[EInt, Int](_ => processEvent(event))(Event.extractor)

    val res = StateMachine[Id].run(pat, Seq(event), pat.initialState())

    res.queue.size shouldBe 0
  }

  it should "process SkipPattern correctly" in {

    val pat = new SimplePattern[EInt, Int](_ => processEvent(event))(Event.extractor)

    val res = StateMachine[Id].run(pat, Seq(event), pat.initialState())

    res.queue.size shouldBe 0
  }

  it should "process ExtractingPattern correctly" in {

    // Decoder Instance
    implicit val dec: Decoder[Int, Int] = ((v: Int) => 2 * v)

    // Pattern Extractor
    implicit val MyExtractor: Extractor[EInt, Symbol, Int] = new Extractor[EInt, Symbol, Int] {
      def apply[T](sym: Symbol)(implicit d: Decoder[Int, T]): EInt => T = _.row
    }

    //val pat = new ExtractingPattern[EInt, Symbol, Int, Int, Int] ('and, 'or)(extractor, MyExtractor, dec)
    val pat = new ExtractingPattern('and)(Event.extractor, MyExtractor, dec)

    val res = StateMachine[Id].run(pat, Seq(event), pat.initialState())

    res.queue.size shouldBe 0
  }

}
