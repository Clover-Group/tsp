package ru.itclover.streammachine

import java.time.Instant
import org.apache.flink.types.Row
import org.scalatest.{FunSuite, Matchers, WordSpec}
import ru.itclover.streammachine.core.PhaseParser
import ru.itclover.streammachine.core.PhaseResult.Success

class EvalTest extends WordSpec with Matchers {
  "Eval" should {
    "simple num. phase" in {
      // TODO
//      val phase: PhaseParser[Row, Any, Any] = EvalUtils.evalPhaseUsingRowExtractors("'speed >= 100.0", 0, Map('speed -> 1))
//      val row = new Row(2)
//      row.setField(0, java.sql.Timestamp.from(Instant.now()))
//      row.setField(1, 200.0)
//      val (result, _) = phase.apply(row, phase.initialState.asInstanceOf[Any])
//
//      result shouldBe a [Success[_]]
    }
  }
}
