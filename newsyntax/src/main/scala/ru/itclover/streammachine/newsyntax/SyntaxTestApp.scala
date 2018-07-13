package ru.itclover.streammachine.newsyntax

import org.parboiled2.{ErrorFormatter, ParseError}

import scala.util.{Failure, Success}

object SyntaxTestApp extends App {
  if (args.length < 1) {
    System.exit(0)
  }
  val parser = new SyntaxParser(args(0))
  val result = parser.start.run()
  result match {
    case Success(x: Expr) => println(x)
    case Failure(x: ParseError) => println(parser.formatError(x, new ErrorFormatter(showTraces = true)))
    case Failure(z) => println(z)
  }
}
