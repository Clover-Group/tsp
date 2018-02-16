package ru.itclover.streammachine

import java.time.DateTimeException
import org.apache.flink.types.Row
import ru.itclover.streammachine.aggregators.AggregatorPhases.Segment
import ru.itclover.streammachine.core.PhaseResult.{Failure, Success, TerminalResult}
import ru.itclover.streammachine.core.Time.TimeExtractor
import ru.itclover.streammachine.io.output.JDBCSegmentsSink


/**
  * Pack PhaseOut into [[org.apache.flink.types.Row]]
  * @tparam Event - inner Event
  */
case class SegmentsToRowResultMapper[Event](schema: JDBCSegmentsSink, patternId: String)
                                           (implicit timeExtractor: TimeExtractor[Row], extractAny: (Event, Symbol) => Any)
     extends ResultMapper[Event, Segment, Row] {
  override def apply(event: Event, results: Seq[TerminalResult[Segment]]) = results map {
    case (Success(segment)) => {
      val resultRow = new Row(schema.fieldsCount)
      // Throw exception here instead of Either or Try due to high computational intensity, O(n).
      val (fromTime, fromMillis) = segment.from.toString.split('.') match {
        case Array(time, millis, _*) => time -> millis.toInt
        case _ => throw new DateTimeException(s"Invalid date format: ${segment.from.toString}, valid format is: yyyy-MM-dd HH:mm:ss.SSS")
      }
      val (toTime, toMillis) = segment.to.toString.split('.') match {
        case Array(time, millis, _*) => time -> millis.toInt
        case _ => throw new DateTimeException(s"Invalid date format: ${segment.to.toString}, valid format is: yyyy-MM-dd HH:mm:ss.SSS")
      }

      resultRow.setField(schema.fromTimeInd, fromTime)
      resultRow.setField(schema.fromTimeMillisInd, fromMillis)
      resultRow.setField(schema.toTimeInd, toTime)
      resultRow.setField(schema.toTimeMillisInd, toMillis)
      resultRow.setField(schema.patternIdInd, patternId)
      schema.forwardedFields.foreach { case (field) =>
        val fieldValue = extractAny(event, field)
        resultRow.setField(schema.fieldsIndexesMap(field), fieldValue)
      }
      Success(resultRow)
    }
    case f@Failure(msg) => f
  }
}
