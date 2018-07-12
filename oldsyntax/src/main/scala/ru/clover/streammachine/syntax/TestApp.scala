package ru.clover.streammachine.syntax

import cats.Eval
import parseback.{LineStream, ParseError, Whitespace}
import parseback.util.Catenable
import shims.syntax.either

object TestApp extends App {
  val testString = "1 + 2 * 3.4"
  val stream: LineStream[Eval] = LineStream[Eval](testString)
  val parser = new UserRuleParser
  implicit val W: Whitespace = parser.W
  val result: either.\/[List[ParseError], Catenable[Expr]] = parser.parser(stream).value
  println(result)
}
