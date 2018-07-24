package ru.itclover.streammachine

import org.scalatest.WordSpec
import ru.itclover.streammachine.core.PhaseParser.Functions._
import ru.itclover.streammachine.core.PhaseResult.{Failure, Stay, Success, TerminalResult}
import ru.itclover.streammachine.core.Time._
import ru.itclover.streammachine.core.{TestPhase, _}
import ru.itclover.streammachine.utils.ParserMatchers
import scala.concurrent.duration._


class ResultMappersTest extends WordSpec with ParserMatchers {


  "SegmentResultsMapper" should {
    implicit def timeExtractor[T] = new TimeExtractor[TestEvent[T]] {
      override def apply(event: TestEvent[T]) = event.time
    }

    "Work on segments and failures" in {
      val results = Seq(Success(Segment(times(0), times(1))), Success(Segment(times(1), times(2))), Failure("Test"),
        Success(Segment(times(3), times(4))), Success(Segment(times(4), times(5))), Failure("Test"))
      val events = results.map { res => TestEvent(res) }

      val resMapper = SegmentResultsMapper[TestEvent[Segment], Segment]()

      val accumulatedSegments = events.flatMap(e => resMapper(e, Seq(e.result.asInstanceOf[TerminalResult[Segment]])))

      accumulatedSegments.length shouldEqual 4
      accumulatedSegments.head shouldEqual Success(Segment(times(0), times(2)))
      accumulatedSegments(2) shouldEqual Success(Segment(times(3), times(5)))

      accumulatedSegments(1) shouldEqual Failure("Test")
      accumulatedSegments(3) shouldEqual Failure("Test")
    }

    "Work on points and failures" in {
      val results = Seq(Success(1), Success(1), Failure("Test"), Success(1), Success(1), Failure("Test"))
      val events = results.zip(times) map { case (res, time) => TestEvent(res, time) }

      val resMapper = SegmentResultsMapper[TestEvent[Int], Int]()
      val accumulatedPoints = events.flatMap(e => resMapper(e, Seq(e.result.asInstanceOf[TerminalResult[Int]])))

      accumulatedPoints.length shouldEqual 4
      accumulatedPoints.head shouldEqual Success(Segment(times(0), times(1)))
      accumulatedPoints(2) shouldEqual Success(Segment(times(3), times(4)))

      accumulatedPoints(1) shouldEqual Failure("Test")
      accumulatedPoints(3) shouldEqual Failure("Test")
    }

  }

}
