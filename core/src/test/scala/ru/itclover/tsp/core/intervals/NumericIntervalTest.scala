package ru.itclover.tsp.core.intervals

import org.scalatest.{Matchers, WordSpec}
import ru.itclover.tsp.core.Intervals.{Inside, NumericInterval}

/**
  * Test class for numeric interval
  */
// In test cases, 'should' expressions are non-unit. Suppressing wartremover warnings about it
@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class NumericIntervalTest extends WordSpec with Matchers {

  "numeric interval" should {

    val testNumericInterval = NumericInterval[Int](
      start = 1000,
      end = Some(4000)
    )

    "check for containing value" in {

      assertResult(expected = true) {
        testNumericInterval.contains(2000)
      }

      assertResult(expected = false) {
        testNumericInterval.contains(100)
      }

    }

    "check if infinite" in {

      assertResult(expected = false) {
        testNumericInterval.isInfinite
      }

    }

    "retrieve relative position" in {
      testNumericInterval.getRelativePosition(item = 2500) shouldBe Inside
    }

  }

}
