package ru.itclover.streammachine.newsyntax

import org.parboiled2.ParseError

import scala.util.{Failure, Success}

object SyntaxTestApp extends App {
  val parser = new SyntaxParser("(x + 1 = y) for 30 seconds")
  val result = parser.start.run()
  result match {
    case Success(x: Expr) => println(x)
    case Failure(x: ParseError) => println(parser.formatError(x))
    case Failure(z) => println(z)
  }
}
