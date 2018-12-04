package ru.itclover.tsp

import java.sql.Timestamp
import java.time.{LocalDateTime, ZonedDateTime, ZoneId}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.types.Row
import ru.itclover.tsp.core.Incident
import ru.itclover.tsp.core.PatternResult.{Failure, Success, TerminalResult}
import ru.itclover.tsp.dsl.schema.RawPattern
import ru.itclover.tsp.io.output.RowSchema
import ru.itclover.tsp.io.{Decoder, Extractor, TimeExtractor}


class ToIncidentsResultMapper[E, EKey, EItem](
  pattern: RawPattern,
  maxWindowMs: Long,
  forwardedFields: Seq[EKey],
  partitionFields: Seq[EKey]
)(implicit timeExtractor: TimeExtractor[E], extractor: Extractor[E, EKey, EItem], decoder: Decoder[EItem, Any])
    extends ResultMapper[E, Segment, Incident] {

  override def apply(event: E, results: Seq[TerminalResult[Segment]]) =
    results map {
      case Success(segment) =>
        Success(
          Incident(
            pattern.id,
            maxWindowMs,
            segment,
            forwardedFields.map(f => f.toString -> extractor[Any](event, f)).toMap,
            pattern.payload,
            partitionFields.map(f => f.toString -> extractor[Any](event, f)).toMap
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
/**/
