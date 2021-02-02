package ru.itclover.tsp.spark.utils

import java.sql.Timestamp
import java.time.{Instant, LocalDateTime, ZoneId, ZonedDateTime}

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import ru.itclover.tsp.core.Incident
import ru.itclover.tsp.spark.io.NewRowSchema

import scala.util.Try

/**
  * Packer of found incident into [[org.apache.spark.sql.Row]]
  */
case class PatternsToRowMapper[Event, EKey](sourceId: Int, schema: NewRowSchema) {

  def map(incident: Incident): Row = {
    val values = new Array[Any](schema.fieldsCount)
    values(schema.unitIdInd) = incident.patternUnit
    values(schema.patternIdInd) = incident.patternId
    values(schema.appIdInd) = schema.appIdFieldVal._2
    values(schema.beginInd) = new Timestamp(incident.segment.from.toMillis)
    values(schema.endInd) = new Timestamp(incident.segment.to.toMillis)
    values(schema.subunitIdInd) = incident.patternSubunit

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

//  def findSubunit(payload: Seq[(String, Any)]): Int = {
//    payload.find { case (name, _) => name.toLowerCase == "subunit" }
//      .map{ case (_, value) => Try(value.toString.toInt).getOrElse(0) }
//      .getOrElse(0)
//  }
}
