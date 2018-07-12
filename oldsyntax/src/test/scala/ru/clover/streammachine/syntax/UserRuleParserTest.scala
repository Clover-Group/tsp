package ru.clover.streammachine.syntax

import cats.Eval
import org.scalatest.matchers.Matcher
import org.scalatest.{FlatSpec, Matchers}
import parseback.util.Catenable
import parseback.{LineStream, ParseError, Parser, Whitespace}
import shims.syntax.either

class UserRuleParserTest extends FlatSpec with Matchers {
  val p = new UserRuleParser

  "Parser" should "parse expression" in {
    //p.parser(LineStream[Eval]("1 + 2 * 3")) should be (1)
  }
}
