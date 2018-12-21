package ru.itclover.tsp.core

import org.scalatest.{Matchers, WordSpec}
import ru.itclover.tsp.core.PatternResult.{Failure, Stay, Success}
import ru.itclover.tsp.patterns.Combining.TogetherParser

class AndPhaseTest extends WordSpec with Matchers {

  "AndParser" should {
    "Success if both side is success" in {
      val andPhase = TogetherParser(alwaysSuccess, alwaysSuccess)

      val (result, _) = andPhase(probe, andPhase.initialState)

      result shouldBe a[Success[_]]
    }

    "Failure if any side is failure" in {
      def andPhaseLeft(right: Pattern[TestEvent[Int], Unit, Int]) = TogetherParser(alwaysFailure, right)

      def andPhaseRight(left: Pattern[TestEvent[Int], Unit, Int]) = TogetherParser(left, alwaysFailure)

      val results = for (secondResult <- Set(alwaysFailure, alwaysSuccess, alwaysStay);
                         parserFunc <- Set(andPhaseLeft _, andPhaseRight _)
      ) yield {
        val phase = parserFunc(secondResult)
        val (result, _) = phase(probe, phase.initialState)
        result
      }

      results foreach (_ shouldBe a[Failure])
    }

    "Stay if (Stay and Stay) | (Success and Stay) | (Stay and Success)" in {
      Seq(
        TogetherParser(alwaysStay, alwaysStay),
        TogetherParser(alwaysSuccess, alwaysStay),
        TogetherParser(alwaysStay, alwaysSuccess)
      ).map {
        phase =>
          val (result, _) = phase(probe, phase.initialState)
          result
      }.foreach(_ shouldBe Stay)
    }
  }

}
