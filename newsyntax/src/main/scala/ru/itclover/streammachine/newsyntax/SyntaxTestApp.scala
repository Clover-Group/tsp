package ru.itclover.streammachine.newsyntax

import java.time.Instant
import org.parboiled2.{ErrorFormatter, ParseError}
import ru.itclover.streammachine.Event
import ru.itclover.streammachine.core.PhaseParser
import ru.itclover.streammachine.core.Time.TimeExtractor
import ru.itclover.streammachine.phases.NumericPhases.SymbolNumberExtractor
import scala.io.StdIn
import scala.util.{Failure, Success}

object SyntaxTestApp extends App {
  implicit val extractTime: TimeExtractor[Event] = new TimeExtractor[Event] {
    override def apply(v1: Event) = v1.time
  }

  implicit val numberExtractor: SymbolNumberExtractor[Event] = new SymbolNumberExtractor[Event] {
    override def extract(event: Event, symbol: Symbol): Double = Double.NaN
  }

  val rule = if (args.length < 1) {
    StdIn.readLine("Enter a rule for testing: ")
  } else {
    args(0)
  }
  val formatter = new ErrorFormatter(showTraces=true)
  val result = PhaseBuilder.build(rule)
  result match {
    case Right(x: (PhaseParser[Event, _, _], ParserMetadata)) =>
      println(x._1.formatWithInitialState(Event(1, Instant.now)))
      println(x._2)
    case Right(z) =>
      println(z)
    case Left(z) =>
      println(z)
  }
}
