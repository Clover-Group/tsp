package ru.itclover.streammachine.newsyntax

import org.parboiled2.{ErrorFormatter, ParseError}
import ru.itclover.streammachine.Event
import ru.itclover.streammachine.core.Time.TimeExtractor

import scala.util.{Failure, Success}

object SyntaxTestApp extends App {
  implicit val extractTime: TimeExtractor[Event] = new TimeExtractor[Event] {
    override def apply(v1: Event) = v1.time
  }

  if (args.length < 1) {
    System.exit(0)
  }
  val parser = new SyntaxParser(args(0))
  val result = parser.start.run()
  result match {
    case Success(x: Expr) =>
      println(x)
      val pb = new PhaseBuilder[Event]
      val pp = pb.build(x)
      print(pp)
    case Failure(x: ParseError) =>
      println(parser.formatError(x, new ErrorFormatter(showTraces = true)))
    case Failure(z) =>
      println(z)
  }
}
