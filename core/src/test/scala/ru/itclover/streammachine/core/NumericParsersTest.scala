package ru.itclover.streammachine.core

import org.scalatest.WordSpec
import ru.itclover.streammachine.phases.MonadPhases
//import ru.itclover.streammachine.core.PhaseParser.Functions._
import ru.itclover.streammachine.core.PhaseResult.{Failure, Success}
//import ru.itclover.streammachine.core.Time._
//import ru.itclover.streammachine.phases.ConstantPhases.ConstantFunctions
//import ru.itclover.streammachine.phases.NumericPhases.SymbolParser
import ru.itclover.streammachine.phases.{ConstantPhases, NoState}
import ru.itclover.streammachine.utils.ParserMatchers
import ru.itclover.streammachine.phases.NumericPhases._
import scala.Predef.{any2stringadd => _, _}

class NumericParsersTest extends WordSpec with ParserMatchers {

  "BinaryNumericParser" should {
    "work on stay and success events and +, -, *" in {
      val a: NumericPhaseParser[TestingEvent[Double], NoState] = ConstantPhases[TestingEvent[Double], Double](10.0)
      checkOnTestEvents(
        (p: TestPhase[Double]) => p + a,
        staySuccesses,
        Seq(Success(11.0), Success(11.0), Success(12.0), Success(12.0), Success(11.0), Success(13.0), Failure("Test"), Success(14.0))
      )
    }
  }
}
