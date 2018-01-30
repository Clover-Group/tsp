package ru.itclover.streammachine.core

import org.scalatest.{Matchers, WordSpec}
import ru.itclover.streammachine.core.PhaseParser.And
import ru.itclover.streammachine.core.PhaseResult.{Failure, Stay, Success}

class AndParserTest extends WordSpec with Matchers {

  "AndParser" should {
    "Success if both side is success" in {
      val andPhase = AndParser(alwaysSuccess, alwaysSuccess)

      val (result, _) = andPhase(probe, andPhase.initialState)

      result shouldBe a[Success[_]]
    }

    "Failure if any side is failure" in {
      def andPhaseLeft(right: PhaseParser[TestEvent, Unit, Int]) = AndParser(alwaysFailure, right)

      def andPhaseRight(left: PhaseParser[TestEvent, Unit, Int]) = AndParser(left, alwaysFailure)

      val results = for (secondResult <- Set(alwaysFailure, alwaysSuccess, alwaysStay);
                         parserFunc <- Set(andPhaseLeft _, andPhaseRight _)
      ) yield {
        val phase: PhaseParser[TestEvent, Unit And Unit, Int And Int] = parserFunc(secondResult)
        val (result, _) = phase(probe, phase.initialState)
        result
      }

      results foreach (_ shouldBe a[Failure])
    }

    "Stay if (Stay and Stay) | (Success and Stay) | (Stay and Success)" in {
      Seq(
        AndParser(alwaysStay, alwaysStay),
        AndParser(alwaysSuccess, alwaysStay),
        AndParser(alwaysStay, alwaysSuccess)
      ).map {
        phase =>
          val (result, _) = phase(probe, phase.initialState)
          result
      }.foreach(_ shouldBe Stay)
    }
  }

}
