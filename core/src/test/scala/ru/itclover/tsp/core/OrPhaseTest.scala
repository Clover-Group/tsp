package ru.itclover.tsp.core

import org.scalatest.{Matchers, WordSpec}
import ru.itclover.tsp.patterns.Combining.EitherParser
import ru.itclover.tsp.core.PatternResult.{Failure, Stay, Success}

class OrPhaseTest extends WordSpec with Matchers {

  "OrParser" should {
    "Failure if both side is failure" in {
      val orPhase = EitherParser(alwaysFailure, alwaysFailure)

      val (result, _) = orPhase(probe, orPhase.initialState)

      result shouldBe a[Failure]
    }

    "Success if any side is success" in {
      def orPhaseLeft(right: Pattern[TestEvent[Int], Unit, Int]) = EitherParser(alwaysSuccess, right)

      def orPhaseRight(left: Pattern[TestEvent[Int], Unit, Int]) = EitherParser(left, alwaysSuccess)

      val results = for (secondResult <- Set(alwaysFailure, alwaysSuccess, alwaysStay);
                         parserFunc <- Set(orPhaseLeft _, orPhaseRight _)
      ) yield {
        val phase = parserFunc(secondResult)
        val (result, _) = phase(probe, phase.initialState)
        result
      }

      results foreach (_ shouldBe a[Success[_]])
    }

    "Stay if (Stay or Stay) | (Failure or Stay) | (Stay or Failure)" in {
      Seq(
        EitherParser(alwaysStay, alwaysStay),
        EitherParser(alwaysFailure, alwaysStay),
        EitherParser(alwaysStay, alwaysFailure)
      ).map {
        phase =>
          val (result, _) = phase(probe, phase.initialState)
          result
      }.foreach(_ shouldBe Stay)
    }
  }

}
