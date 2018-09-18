package ru.itclover.tsp.dsl

import java.time.Instant
import org.parboiled2.{ErrorFormatter, ParseError}
import ru.itclover.tsp.core.Pattern
import ru.itclover.tsp.core.Time.TimeExtractor
import ru.itclover.tsp.phases.NumericPhases.SymbolNumberExtractor
import ru.itclover.tsp.TestApp.TestEvent
import scala.io.StdIn
import scala.util.{Failure, Success}

object SyntaxTestApp extends App {
  // FIXME Replace TestApp.TestEvent with core.TestEvent[T]
  implicit val extractTime: TimeExtractor[TestEvent] = new TimeExtractor[TestEvent] {
    override def apply(v1: TestEvent) = v1.time
  }

  implicit val numberExtractor: SymbolNumberExtractor[TestEvent] = new SymbolNumberExtractor[TestEvent] {
    override def extract(event: TestEvent, symbol: Symbol): Double = Double.NaN
  }

  val rule = if (args.length < 1) {
    StdIn.readLine("Enter a rule for testing: ")
  } else {
    args(0)
  }
  val formatter = new ErrorFormatter(showTraces = true)
  val result = PhaseBuilder.build(rule)
  result match {
    case Right((x: Pattern[TestEvent, _, _], m: PhaseMetadata)) =>
      println(x.format(TestEvent(1, Instant.now)))
      println(m)
    case Right(z) =>
      println(z)
    case Left(z) =>
      println(z)
  }
}
