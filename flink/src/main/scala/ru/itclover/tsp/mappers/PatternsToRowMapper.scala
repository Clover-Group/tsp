package ru.itclover.tsp.mappers

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneId, ZonedDateTime}

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.types.Row
import ru.itclover.tsp.core.Incident
import ru.itclover.tsp.io.output.RowSchema

/**
  * Packer of found incident into [[org.apache.flink.types.Row]]
  */
case class PatternsToRowMapper[Event, EKey](sourceId: Int, schema: RowSchema) extends RichMapFunction[Incident, Row] {

  override def map(incident: Incident) = {
    val resultRow = new Row(schema.fieldsCount)
    resultRow.setField(schema.sourceIdInd, sourceId)
    resultRow.setField(schema.patternIdInd, incident.patternId)
    resultRow.setField(schema.appIdInd, schema.appIdFieldVal._2)
    resultRow.setField(schema.beginInd, incident.segment.from.toMillis / 1000.0)
    resultRow.setField(schema.endInd, incident.segment.to.toMillis / 1000.0)
    resultRow.setField(schema.processingTimeInd, nowInUtcMillis)

    val payload = incident.forwardedFields ++ incident.patternPayload
    resultRow.setField(schema.contextInd, payloadToJson(payload))

    resultRow
  }

  def nowInUtcMillis: Double = {
    val zonedDt = ZonedDateTime.of(LocalDateTime.now, ZoneId.systemDefault)
    val utc = zonedDt.withZoneSameInstant(ZoneId.of("UTC"))
    Timestamp.valueOf(utc.toLocalDateTime).getTime / 1000.0
  }

  def payloadToJson(payload: Seq[(String, Any)]): String =
    payload
      .map {
        case (fld, value) if value.isInstanceOf[String] => s""""${fld}":"${value}""""
        case (fld, value)                               => s""""${fld}":$value"""
      }
      .mkString("{", ",", "}")
}
