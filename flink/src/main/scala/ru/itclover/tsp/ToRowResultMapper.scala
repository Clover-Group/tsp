package ru.itclover.tsp

import java.time._
import java.sql.Timestamp
import java.time.DateTimeException
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.types.Row
import ru.itclover.tsp.core.Incident
import ru.itclover.tsp.core.PatternResult.{Failure, Success, TerminalResult}
import ru.itclover.tsp.core.Time.TimeExtractor
import ru.itclover.tsp.io.input.RawPattern
import ru.itclover.tsp.io.output.RowSchema


// .. TODO Set into flat mapper
class ToIncidentsResultMapper[Event](
  patternId: String,
  maxWindowMs: Long,
  forwardedFields: Seq[Symbol],
  partitionFields: Seq[Symbol]
)(implicit timeExtractor: TimeExtractor[Event], extractAny: (Event, Symbol) => Any)
    extends ResultMapper[Event, Segment, Incident] {

  override def apply(event: Event, results: Seq[TerminalResult[Segment]]) =
    results map {
      case Success(segment) =>
        Success(
          Incident(
            patternId,
            maxWindowMs,
            segment,
            forwardedFields.map(f => f -> extractAny(event, f)).toMap,
            partitionFields.map(f => f -> extractAny(event, f)).toMap
          )
        )
      case f: Failure => f
    }
}

case class PatternsToRowMapper[Event](sourceId: Int, schema: RowSchema) extends RichMapFunction[Incident, Row] {

  override def map(foundPattern: Incident) = {
    val resultRow = new Row(schema.fieldsCount)
    resultRow.setField(schema.sourceIdInd, sourceId)
    resultRow.setField(schema.patternIdInd, foundPattern.id)
    resultRow.setField(schema.appIdInd, schema.appIdFieldVal._2)
    resultRow.setField(schema.beginInd, foundPattern.segment.from.toMillis / 1000.0)
    resultRow.setField(schema.endInd, foundPattern.segment.to.toMillis / 1000.0)
    resultRow.setField(schema.processingTimeInd, nowInUtcMillis)

    val payload = (foundPattern.partitionFields ++ foundPattern.forwardedFields) map {
      case (f, v) => f.toString.tail -> v
    }
    resultRow.setField(schema.contextInd, payloadToJson(payload.toSeq))

    resultRow
  }

  def nowInUtcMillis: Double = {
    val zonedDt = ZonedDateTime.of(LocalDateTime.now, ZoneId.systemDefault)
    val utc = zonedDt.withZoneSameInstant(ZoneId.of("UTC"))
    Timestamp.valueOf(utc.toLocalDateTime).getTime / 1000.0
  }

  def payloadToJson(payload: Seq[(String, Any)]): String = {
    payload.map {
      case (fld, value) if value.isInstanceOf[String] => s""""${fld}":"${value}""""
      case (fld, value)                               => s""""${fld}":$value"""
    } mkString ("{", ",", "}")
  }
}

/**
  * Packer of PhaseOut into [[org.apache.flink.types.Row]]
  * @tparam Event - inner Event
  */
class ToRowResultMapper[Event](sourceId: Int, schema: RowSchema, pattern: RawPattern)(
  implicit timeExtractor: TimeExtractor[Row],
  extractAny: (Event, Symbol) => Any
) extends ResultMapper[Event, Segment, Row] {

  // TODO: replace mappers with result_phase on parser like `SELECT [result_phase] WHERE [phase]`
  // Segments closes on next after last success event, hence forwarded fields should be got from prev successful Event.
  var prevEvent: Option[Event] = None

  override def apply(event: Event, results: Seq[TerminalResult[Segment]]) = {
    val packedEvent = results map {
      case (Success(segment)) => {
        val resultRow = new Row(schema.fieldsCount)
        resultRow.setField(schema.sourceIdInd, sourceId)
        resultRow.setField(schema.patternIdInd, pattern.id)
        resultRow.setField(schema.appIdInd, schema.appIdFieldVal._2)
        resultRow.setField(schema.beginInd, segment.from.toMillis / 1000.0)
        resultRow.setField(schema.endInd, segment.to.toMillis / 1000.0)
        resultRow.setField(schema.processingTimeInd, nowInUtcMillis)

        val forwardedFields = pattern.forwardedFields ++ schema.forwardedFields
        val payload = forwardedFields.map(f => f.toString.tail -> extractAny(prevEvent.getOrElse(event), f)) ++
        pattern.payload.toSeq
        resultRow.setField(schema.contextInd, payloadToJson(payload))

        Success(resultRow)
      }
      case f @ Failure(msg) => f
    }
    prevEvent = Some(event)
    packedEvent
  }

  def nowInUtcMillis: Double = {
    val zonedDt = ZonedDateTime.of(LocalDateTime.now, ZoneId.systemDefault)
    val utc = zonedDt.withZoneSameInstant(ZoneId.of("UTC"))
    Timestamp.valueOf(utc.toLocalDateTime).getTime / 1000.0
  }

  def payloadToJson(payload: Seq[(String, Any)]): String = {
    payload.map {
      case (fld, value) if value.isInstanceOf[String] => s""""${fld}":"${value}""""
      case (fld, value)                               => s""""${fld}":$value"""
    } mkString ("{", ",", "}")
  }
}
