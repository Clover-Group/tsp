package ru.itclover.tsp.spark.utils

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneId, ZonedDateTime}

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import ru.itclover.tsp.core.Incident
import ru.itclover.tsp.spark.io.RowSchema

/**
  * Packer of found incident into [[org.apache.spark.sql.Row]]
  */
case class PatternsToRowMapper[Event, EKey](sourceId: Int, schema: RowSchema) {

  def map(incident: Incident): Row = {
    val values = new Array[Any](schema.fieldsCount)
    values(schema.sourceIdInd) = sourceId
    values(schema.patternIdInd) = incident.patternId
    values(schema.appIdInd) = schema.appIdFieldVal._2
    values(schema.beginInd) = incident.segment.from.toMillis / 1000.0
    values(schema.endInd) = incident.segment.to.toMillis / 1000.0
    values(schema.processingTimeInd) = nowInUtcMillis

    val payload = incident.forwardedFields ++ incident.patternPayload
    values(schema.contextInd) = payloadToJson(payload)

    new GenericRow(values)
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
