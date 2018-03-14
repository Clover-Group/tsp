package ru.itclover.streammachine.utils

import com.typesafe.scalalogging.Logger
import org.scalatest.Matchers
import ru.itclover.streammachine.core.PhaseResult.{Failure, Success}
import ru.itclover.streammachine.core.{PhaseParser, PhaseResult, TestPhase, TestingEvent, runRule}


trait ParserMatchers extends Matchers {
  val log = Logger[ParserMatchers]

  def checkOnTestEvents[InnerOut, Out: Numeric](
      parser: TestPhase[InnerOut] => PhaseParser[TestingEvent[InnerOut], _, Out],
      events: Seq[TestingEvent[InnerOut]],
      expectedResults: Seq[PhaseResult[Out]],
      epsilon: Out = 0.00001
  ): Unit = {
    val rule = parser(TestPhase())
    val results = runRule(rule, events)
    results.length should equal(expectedResults.length)
    results.zip(expectedResults) map {
      case (Success(real), Success(exp)) => real shouldEqual (exp +- epsilon)
      case (Failure(_), Failure("")) => ()
      case (real, exp) if real == exp => ()
      case (real, exp) => {
        log.debug(s"results = `$results`\nexpected = `$expectedResults`")
        fail(s"Expected ($exp) and real ($real) result is different.")
      }
    }
  }

  def checkOnTestEvents_strict[InnerOut, Out](
      parser: TestPhase[InnerOut] => PhaseParser[TestingEvent[InnerOut], _, Out],
      events: Seq[TestingEvent[InnerOut]],
      expectedResults: Seq[PhaseResult[Out]]
  ): Unit = {
    val rule = parser(TestPhase())
    val results = runRule(rule, events)
    results.length should equal(expectedResults.length)
    results.zip(expectedResults) map {
      case (Success(real), Success(exp)) => real shouldEqual exp
      case (Failure(_), Failure("")) => ()
      case (real, exp) if real == exp => ()
      case (real, exp) => fail(s"Expected ($exp) and real ($real) result is different.")
    }
  }

}
