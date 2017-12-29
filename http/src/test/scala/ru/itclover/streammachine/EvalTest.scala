package ru.itclover.streammachine

import org.apache.flink.types.Row
import org.scalatest.{FunSuite, Matchers, WordSpec}
import ru.itclover.streammachine.core.PhaseResult.Success
import ru.itclover.streammachine.utils.EvalUtils

class EvalTest extends WordSpec with Matchers {
  "Eval" should {
    "simple num. phase" in {
      val phase = EvalUtils.evalPhaseUsingRowExtractors("'speed > 100", 0, Map('speed -> 1))
      val row = new Row(1)
      row.setField(0, 200)
//      val (result, _) = phase(row, phase.initialState)
//
//      result shouldBe a [Success[Boolean]]
    }
  }
}
