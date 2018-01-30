package ru.itclover.streammachine.core

import org.scalatest.{Matchers, WordSpec}
import ru.itclover.streammachine.core.PhaseParser.{And, Or}
import ru.itclover.streammachine.core.PhaseResult.{Failure, Stay, Success}

class OrParserTest extends WordSpec with Matchers {

  "OrParser" should {
    "Failure if both side is failure" in {
      val orPhase = OrParser(alwaysFailure, alwaysFailure)

      val (result, _) = orPhase(probe, orPhase.initialState)

      result shouldBe a[Failure]
    }

    "Success if any side is success" in {
      def orPhaseLeft(right: PhaseParser[TestEvent, Unit, Int]) = OrParser(alwaysSuccess, right)

      def orPhaseRight(left: PhaseParser[TestEvent, Unit, Int]) = OrParser(left, alwaysSuccess)

      val results = for (secondResult <- Set(alwaysFailure, alwaysSuccess, alwaysStay);
                         parserFunc <- Set(orPhaseLeft _, orPhaseRight _)
      ) yield {
        val phase: PhaseParser[TestEvent, Unit And Unit, Int Or Int] = parserFunc(secondResult)
        val (result, _) = phase(probe, phase.initialState)
        result
      }

      results foreach (_ shouldBe a[Success[_]])
    }

    "Stay if (Stay or Stay) | (Failure or Stay) | (Stay or Failure)" in {
      Seq(
        OrParser(alwaysStay, alwaysStay),
        OrParser(alwaysFailure, alwaysStay),
        OrParser(alwaysStay, alwaysFailure)
      ).map {
        phase =>
          val (result, _) = phase(probe, phase.initialState)
          result
      }.foreach(_ shouldBe Stay)
    }
  }

}
