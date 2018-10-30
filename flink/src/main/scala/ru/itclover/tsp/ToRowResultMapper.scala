package ru.itclover.tsp

import java.time._
import java.sql.Timestamp
import java.time.DateTimeException

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.types.Row
import ru.itclover.tsp.core.Incident
import ru.itclover.tsp.core.PatternResult.{Failure, Success, TerminalResult}
import ru.itclover.tsp.core.Time.TimeExtractor
import ru.itclover.tsp.dsl.schema.RawPattern
import ru.itclover.tsp.io.output.RowSchema
import ru.itclover.tsp.phases.Phases.AnyExtractor


class ToIncidentsResultMapper[Event](
  pattern: RawPattern,
  maxWindowMs: Long,
  forwardedFields: Seq[Symbol],
  partitionFields: Seq[Symbol]
)(implicit timeExtractor: TimeExtractor[Event], extractAny: AnyExtractor[Event])
    extends ResultMapper[Event, Segment, Incident] {

  override def apply(event: Event, results: Seq[TerminalResult[Segment]]) =
    results map {
      case Success(segment) =>
        Success(
          Incident(
            pattern.id,
            maxWindowMs,
            segment,
            forwardedFields.map(f => f -> extractAny(event, f)).toMap,
            pattern.payload,
            partitionFields.map(f => f -> extractAny(event, f)).toMap
          )
        )
      case f: Failure => f
    }
}

/**
  * Packer of found incident into [[org.apache.flink.types.Row]]
  */
case class PatternsToRowMapper(sourceId: Int, schema: RowSchema) extends RichMapFunction[Incident, Row] {

  override def map(incident: Incident) = {
    val resultRow = new Row(schema.fieldsCount)
    resultRow.setField(schema.sourceIdInd, sourceId)
    resultRow.setField(schema.patternIdInd, incident.id)
    resultRow.setField(schema.appIdInd, schema.appIdFieldVal._2)
    resultRow.setField(schema.beginInd, incident.segment.from.toMillis / 1000.0)
    resultRow.setField(schema.endInd, incident.segment.to.toMillis / 1000.0)
    resultRow.setField(schema.processingTimeInd, nowInUtcMillis)

    val payload = ((incident.partitionFields ++ incident.forwardedFields) map {
      case (f, v) => f.toString.tail -> v
    }) ++ incident.patternPayload
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
