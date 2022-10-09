package ru.itclover.tsp.core.intervals

import org.scalatest.wordspec._

import org.scalatest.matchers.should._
import ru.itclover.tsp.core.Intervals.{Inside, TimeInterval}

/**
  * Test class for time interval
  */
// In test cases, 'should' expressions are non-unit. Suppressing wartremover warnings about it
@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class TimeIntervalTest extends AnyWordSpec with Matchers {

  "time interval" should {

    val testTimeInterval = TimeInterval(
      min = 1000L,
      max = 4000L
    )

    "check for containing value" in {

      assertResult(expected = true) {
        testTimeInterval.contains(2000)
      }

      assertResult(expected = false) {
        testTimeInterval.contains(100)
      }

    }

    "check if infinite" in {

      assertResult(expected = false) {
        testTimeInterval.isInfinite
      }

    }

    "retrieve relative position" in {
      testTimeInterval.getRelativePosition(item = 2500L) shouldBe Inside
    }

    "calculate midpoint" in {

      assertResult(2500L) {
        testTimeInterval.midpoint
      }

    }

  }

}
