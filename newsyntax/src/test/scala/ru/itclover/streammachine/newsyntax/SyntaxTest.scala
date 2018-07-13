package ru.itclover.streammachine.newsyntax

import org.parboiled2.ParseError
import org.scalatest.{FlatSpec, Matchers}

import scala.util.{Failure, Success}

class SyntaxTest extends FlatSpec with Matchers {
  val validRule = "(x > 1) for 5 seconds andthen (y < 2)"
  val invalidRule = "1 = invalid rule"

  "Parser" should "parse rule" in {
    val p = new SyntaxParser(validRule)
    p.start.run() shouldBe a [Success[_]]
  }

  "Parser" should "not parse invalid rule" in {
    val p = new SyntaxParser(invalidRule)
    p.start.run() shouldBe a [Failure[_]]
  }
}
