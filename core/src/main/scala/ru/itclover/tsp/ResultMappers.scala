package ru.itclover.tsp

import com.typesafe.scalalogging.Logger
import ru.itclover.tsp.core.PatternResult
import ru.itclover.tsp.core.PatternResult.{Success, TerminalResult}
import ru.itclover.tsp.core.Time.timeOrdering
import ru.itclover.tsp.io.TimeExtractor

/**
  * Used for statefully process result inside of each [[PatternMapper.apply]] with Event.
 *
  * @tparam Event     - inner Event
  * @tparam PhaseOut  - result of [[PatternMapper.apply]]
  * @tparam MapperOut - resulting results sequence
  */
trait ResultMapper[Event, PhaseOut, MapperOut] extends
  ((Event, Seq[TerminalResult[PhaseOut]]) => Seq[TerminalResult[MapperOut]]) with Serializable


object ResultMapper {

  implicit class ResultMapperRich[Event, PhaseOut, MapperOut](val mapper: ResultMapper[Event, PhaseOut, MapperOut]) extends AnyVal {

    def andThen[Mapper2Out](secondMapper: ResultMapper[Event, MapperOut, Mapper2Out]):
      AndThenResultsMapper[Event, PhaseOut, MapperOut, Mapper2Out] = AndThenResultsMapper(mapper, secondMapper)
  }
}


case class FakeMapper[Event, PhaseOut]() extends ResultMapper[Event, PhaseOut, PhaseOut] {
  def apply(event: Event, results: Seq[TerminalResult[PhaseOut]]): Seq[TerminalResult[PhaseOut]] = results
}


case class AndThenResultsMapper[Event, PhaseOut, Mapper1Out, Mapper2Out](first: ResultMapper[Event, PhaseOut, Mapper1Out],
                                                                         second: ResultMapper[Event, Mapper1Out, Mapper2Out])
  extends ResultMapper[Event, PhaseOut, Mapper2Out] {

  override def apply(e: Event, r: Seq[TerminalResult[PhaseOut]]): Seq[TerminalResult[Mapper2Out]] = second(e, first(e, r))
}


// TODO update rules tests which use it and remove
/** Stateful. Accumulate segments in one. */
case class SegmentResultsMapper[Event, PhaseOut](implicit val extractTime: TimeExtractor[Event])
  extends ResultMapper[Event, PhaseOut, Segment]
{
  var currSegmentOpt: Option[Segment] = None

  // val log = Logger("SegmentResultsMapper")

  def apply(event: Event, results: Seq[TerminalResult[PhaseOut]]): Seq[TerminalResult[Segment]] = {
    val eventTime = extractTime(event)
    val (successes, failures) = results.partition(_.isInstanceOf[Success[PhaseOut]])
    val failuresResults = failures.map(_.asInstanceOf[TerminalResult[Segment]])
    // If results successful and they are present - accumulate it
    if (successes.nonEmpty && !failures.contains(PatternResult.heartbeat)) {
      // log.debug(s"Successes = ${successes.mkString}")
      val segment = successes.head match {
        case Success(s: Segment) => currSegmentOpt.getOrElse(s)
        case _ => currSegmentOpt.getOrElse(Segment(eventTime, eventTime))
      }
      // Accumulate results if it already segmented (Stay-segmented)
      currSegmentOpt = Some(successes.foldLeft(segment) { (segment, result) =>
        result match {
          case Success(Segment(from, to)) =>
            Segment(timeOrdering.min(from, segment.from), timeOrdering.max(to, segment.to))
          case x =>
            segment.copy(to = timeOrdering.max(segment.to, eventTime))
        }
      })
      failuresResults
    }
    // else clear currSegmentOpt and return accumulated segment.
    else {
      if (failures.nonEmpty) {
        val segmentSeq = currSegmentOpt match {
          case Some(segment) => Seq(Success(segment.copy()))
          case None => Seq.empty
        }
        currSegmentOpt = None
        segmentSeq ++ failuresResults
      } else {
        Nil // or, if failures also empty - do not clear currSegmentOpt.
      }
    }
  }
}
