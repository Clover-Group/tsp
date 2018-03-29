package ru.itclover.streammachine

import java.time._
import java.sql.Timestamp
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
case class SegmentsToRowResultMapper[Event](schema: JDBCSegmentsSink, patternId: String, ctxWriter: ContextWriter = JsonbWriter())
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

      resultRow.setField(schema.sourceIdInd, schema.sourceId)
      resultRow.setField(schema.patternIdInd, patternId)
      resultRow.setField(schema.appIdInd, schema.appId)
      resultRow.setField(schema.beginInd, segment.from.toMillis / 1000.0)
      resultRow.setField(schema.endInd, segment.to.toMillis / 1000.0)
      resultRow.setField(schema.processingTimeInd, nowInUtcMillis)

      val context = ctxWriter.write(schema.forwardedFields.map(f => f -> extractAny(event, f)))
      resultRow.setField(schema.contextInd, context)

      Success(resultRow)
    }
    case f@Failure(msg) =>
      f
  }

  def nowInUtcMillis: Double = {
    val zonedDt = ZonedDateTime.of(LocalDateTime.now, ZoneId.systemDefault)
    val utc = zonedDt.withZoneSameInstant(ZoneId.of("UTC"))
    Timestamp.valueOf(utc.toLocalDateTime).getTime / 1000.0  // TODO check correctness
  }
}


trait ContextWriter {
  def write(fieldsValues: Seq[(Symbol, Any)]): String
}

case class JsonbWriter() extends ContextWriter {
  override def write(fieldsValues: Seq[(Symbol, Any)]): String = {
    fieldsValues.map {
      case (fld, value) if value.isInstanceOf[String] => s""""${fld}": "${value}""""
      case (fld, value) => s""""${fld}": $value"""
    } mkString("{", ", ", "}")
  }
}
