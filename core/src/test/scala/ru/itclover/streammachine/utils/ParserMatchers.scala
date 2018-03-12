package ru.itclover.streammachine.utils

import org.scalatest.Matchers
import ru.itclover.streammachine.core.PhaseResult.{Failure, Stay, Success, TerminalResult}
import ru.itclover.streammachine.core.{PhaseParser, PhaseResult, TestPhase, TestingEvent, TimedEvent, runRule}
import ru.itclover.streammachine.phases.NumericPhases.NumericPhaseParser


trait ParserMatchers extends Matchers {

  def checkOnTestEvents[InnerOut](
      parser: TestPhase[InnerOut] => PhaseParser[TestingEvent[InnerOut], _, Double],
      events: Seq[TestingEvent[InnerOut]],
      expectedResults: Seq[PhaseResult[Double]],
      epsilon: Double = 0.00001
  ): Unit = {
    val rule = parser(TestPhase())
    val results = runRule(rule, events)
    results.length should equal(expectedResults.length)
    results.zip(expectedResults) map {
      case (Success(real), Success(exp)) => real shouldEqual (exp +- epsilon)
      case (Failure(_), Failure("")) => ()
      case (real, exp) if real == exp => ()
      case (real, exp) => fail(s"Expected ($exp) and real ($real) result is different.")
    }
  }

}
