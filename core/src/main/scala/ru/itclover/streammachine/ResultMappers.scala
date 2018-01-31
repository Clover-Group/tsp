package ru.itclover.streammachine

import ru.itclover.streammachine.aggregators.AggregatorPhases.Segment
import ru.itclover.streammachine.core.PhaseResult.{Success, TerminalResult}
import ru.itclover.streammachine.core.Time.{TimeExtractor, timeOrdering}


trait ResultMapper[Event, PhaseOut, MapperOut] extends
  ((Event, Seq[TerminalResult[PhaseOut]]) => Seq[TerminalResult[MapperOut]]) with Serializable


case class FakeMapper[Event, PhaseOut]() extends ResultMapper[Event, PhaseOut, PhaseOut] {
  def apply(event: Event, results: Seq[TerminalResult[PhaseOut]]): Seq[TerminalResult[PhaseOut]] = results
}


/** Stateful. Emit Success even if Failure also present in results. */
case class SegmentResultsMapper[Event, PhaseOut](implicit val extractTime: TimeExtractor[Event])
  extends ResultMapper[Event, PhaseOut, Segment]
{
  // TODO(0): distribute (open()?)
  var currSegmentOpt: Option[Segment] = None

  def apply(event: Event, results: Seq[TerminalResult[PhaseOut]]): Seq[TerminalResult[Segment]] = {
    val eventTime = extractTime(event)
    val (successes, failures) = results.partition(_.isInstanceOf[Success[PhaseOut]])
    val failuresResults = failures.map(_.asInstanceOf[TerminalResult[Segment]])
    // If results are present - accumulate it
    if (successes.nonEmpty) {
      val segment = currSegmentOpt.map(_.copy(to = eventTime)).getOrElse(Segment(eventTime, eventTime))
      // Accumulate results if it already segmented (Stay-segmented)
      currSegmentOpt = Some(successes.foldLeft(segment) { (segment, result) =>
        result match  {
          case Success(Segment(from, to)) =>
            Segment(timeOrdering.min(from, segment.from), timeOrdering.max(to, segment.to))
          case _ => segment // todo: separate segmentated out and not on type-level
        }
      })
      failuresResults
    }
    // else clear currSegmentOpt and return accumulated segment.
    else {
      val segmentSeq = currSegmentOpt match {
        case Some(segment) => Traversable(Success(segment.copy())).toSeq
        case None => Seq.empty
      }
      currSegmentOpt = None
      segmentSeq ++ failuresResults
    }
  }
}
