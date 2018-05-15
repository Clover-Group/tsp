package ru.itclover.streammachine

import java.time._
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.DateTimeException
import org.apache.flink.types.Row
import ru.itclover.streammachine.aggregators.AggregatorPhases.Segment
import ru.itclover.streammachine.core.PhaseResult.{Failure, Success, TerminalResult}
import ru.itclover.streammachine.core.Time.TimeExtractor
import ru.itclover.streammachine.io.input.RawPattern
import ru.itclover.streammachine.io.output.JDBCSegmentsSink


/**
  * Pack PhaseOut into [[org.apache.flink.types.Row]]
  * @tparam Event - inner Event
  */
case class SegmentsToRowResultMapper[Event](sourceId: Int, schema: JDBCSegmentsSink, pattern: RawPattern)
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

      resultRow.setField(schema.sourceIdInd, sourceId)
      resultRow.setField(schema.patternIdInd, pattern.id)
      resultRow.setField(schema.appIdInd, schema.appIdFieldVal._2)
      resultRow.setField(schema.beginInd, segment.from.toMillis / 1000.0)
      resultRow.setField(schema.endInd, segment.to.toMillis / 1000.0)
      resultRow.setField(schema.processingTimeInd, nowInUtcMillis)

      val payload = schema.forwardedFields.map(f => f.toString.tail -> extractAny(event, f)) ++ pattern.payload.toSeq
      resultRow.setField(schema.contextInd, payloadToJson(payload))

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

  def payloadToJson(payload: Seq[(String, Any)]): String = {
    payload.map {
      case (fld, value) if value.isInstanceOf[String] => s""""${fld}":"${value}""""
      case (fld, value) => s""""${fld}": $value"""
    } mkString("{", ",", "}")
  }
}