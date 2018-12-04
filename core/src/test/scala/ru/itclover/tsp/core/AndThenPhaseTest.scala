package ru.itclover.tsp.core

import org.scalatest.{Matchers, WordSpec}
import ru.itclover.tsp.core.PatternResult.{Failure, Success}
import ru.itclover.tsp.patterns.Combining.AndThenParser

class AndThenPhaseTest extends WordSpec with Matchers {

  "AndThenParser" should {
    "Success followed by Success == Success" in {
      val andThenParser = AndThenParser(alwaysSuccess, alwaysSuccess)

      val terminalResults = runRule(andThenParser, Seq(probe, probe))

      terminalResults shouldNot be(empty)
      terminalResults.foreach(_ shouldBe a[Success[_]])
    }

    "Success followed by Failure == Failure" in {
      val andThenParser = AndThenParser(alwaysFailure, alwaysFailure)

      val terminalResults = runRule(andThenParser, Seq(probe, probe))
      terminalResults.foreach(_ shouldBe a[Failure])

    }
  }

}
