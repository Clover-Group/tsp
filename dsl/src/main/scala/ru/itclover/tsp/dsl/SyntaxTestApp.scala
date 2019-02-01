package ru.itclover.tsp.dsl

import java.time.Instant

import org.parboiled2.{ErrorFormatter, ParseError}
import ru.itclover.tsp.core.Pattern
import v2.{AST, ASTBuilder}

import scala.io.StdIn
import scala.reflect.ClassTag

object SyntaxTestApp extends App {
  // FIXME Replace TestApp.TestEvent with core.TestEvent[T]

  val rule = if (args.length < 1) {
    StdIn.readLine("Enter a rule for testing: ")
  } else {
    args(0)
  }
  val formatter = new ErrorFormatter(showTraces = true)
  val result = new ASTBuilder(rule, 0.0, Map.empty.withDefaultValue(ClassTag(classOf[Double]))).start.run().toEither
  result match {
    case Right(x: AST) =>
      println(x)
    //println(m)
    case Right(z) =>
      println(z)
    case Left(e: ParseError) =>
      println(e.format(rule, formatter))
    case Left(z) =>
      println(z)
  }
}
