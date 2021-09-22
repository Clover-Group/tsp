package ru.itclover.tsp.core.patterns

import org.scalatest.{Matchers, WordSpec}
import ru.itclover.tsp.core.{Incident, IncidentInstances, Segment, Time}

import java.util.UUID

/**
  * Test class for founded pattern
  */
class FoundedPatternTest extends WordSpec with Matchers {

  "retrieve incident" should {

    val firstTestSegment = Segment(
      from = Time(1000),
      to = Time(4000)
    )

    val secondTestSegment = Segment(
      from = Time(2000),
      to = Time(3000)
    )

    "from semigroup" in {

      val firstIncident = Incident(
        id = "first",
        incidentUUID = UUID.fromString("00000000-0000-0000-0000-000000000001"),
        patternId = 1,
        maxWindowMs = 1000,
        segment = firstTestSegment,
        forwardedFields = Seq(("test1", "1"), ("test2", "2")),
        patternUnit = 13,
        patternSubunit = 42,
        patternPayload = Seq(("test3", "3"), ("test4", "4"))
      )

      val secondIncident = Incident(
        id = "second",
        incidentUUID = UUID.fromString("00000000-0000-0000-0000-000000000002"),
        patternId = 2,
        maxWindowMs = 4000,
        segment = secondTestSegment,
        forwardedFields = Seq(("test1", "1"), ("test2", "2")),
        patternUnit = 13,
        patternSubunit = 42,
        patternPayload = Seq(("test3", "3"), ("test4", "4"))
      )

      val expectedIncident = Incident(
        id = "second",
        incidentUUID = UUID.fromString("00000000-0000-0000-0000-000000000002"),
        patternId = 2,
        maxWindowMs = 4000,
        segment = Segment(
          from = Time(1000),
          to = Time(4000)
        ),
        forwardedFields = Seq(("test1", "1"), ("test2", "2")),
        patternUnit = 13,
        patternSubunit = 42,
        patternPayload = Seq(("test3", "3"), ("test4", "4"))
      )

      val actualIncident = IncidentInstances.semigroup.combine(firstIncident, secondIncident)

      actualIncident shouldBe expectedIncident

    }

  }

}
