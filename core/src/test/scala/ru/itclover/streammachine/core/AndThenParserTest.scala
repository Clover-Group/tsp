package ru.itclover.streammachine.core

import org.scalatest.{Matchers, WordSpec}
import ru.itclover.streammachine.core.PhaseResult.{Failure, Success}
import ru.itclover.streammachine.phases.CombiningPhases.AndThenParser

class AndThenParserTest extends WordSpec with Matchers {

  // TODO(1)
  /*"AndThenParser" should {
    "Success followed by Success == Success" in {
      val andThenParser = AndThenParser(alwaysSuccess, alwaysSuccess)

      val terminalResults = runRule(andThenParser, Seq(probe, probe))

//      println(terminalResults)

      terminalResults shouldNot be(empty)
//      terminalResults.length shouldEqual 1
      terminalResults.foreach(_ shouldBe a[Success[_]])
    }

    "Success followed by Failure == Failure" in {
      val andThenParser = AndThenParser(alwaysFailure, alwaysFailure)

      val terminalResults = runRule(andThenParser, Seq(probe, probe))
      terminalResults.foreach(_ shouldBe a[Failure])

    }

    "Failure followed by Any == Failure" in {}

  }*/

}
