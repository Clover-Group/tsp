package ru.itclover.tsp.mappers

import java.sql.Timestamp
import java.time.{Instant, LocalDateTime, ZoneId, ZoneOffset, ZonedDateTime}

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.types.Row
import ru.itclover.tsp.core.Incident
import ru.itclover.tsp.io.output.{EventSchema, NewRowSchema}

import scala.util.Try

/**
  * Packer of found incident into [[org.apache.flink.types.Row]]
  */
case class PatternsToRowMapper[Event, EKey](sourceId: Int, schema: EventSchema) extends RichMapFunction[Incident, Row] {

  override def map(incident: Incident) = schema match {
    case newRowSchema: NewRowSchema =>
      val resultRow = new Row(newRowSchema.fieldsCount)
      resultRow.setField(newRowSchema.unitIdInd, incident.forwardedFields.find(_._1 == newRowSchema.unitIdField.name).map(_._2.toString.toInt).getOrElse(0))
      resultRow.setField(newRowSchema.patternIdInd, incident.patternId)
      resultRow.setField(newRowSchema.appIdInd, newRowSchema.appIdFieldVal._2)
      resultRow.setField(newRowSchema.beginInd, Timestamp.from(Instant.ofEpochMilli(incident.segment.from.toMillis)))
      resultRow.setField(newRowSchema.endInd, Timestamp.from(Instant.ofEpochMilli(incident.segment.from.toMillis)))
      resultRow.setField(newRowSchema.subunitIdInd, incident.patternSubunit)
      resultRow.setField(newRowSchema.incidentIdInd, incident.incidentUUID.toString)

      resultRow
  }

  def nowInUtcMillis: Double = {
    val zonedDt = ZonedDateTime.of(LocalDateTime.now, ZoneId.systemDefault)
    val utc = zonedDt.withZoneSameInstant(ZoneId.of("UTC"))
    Timestamp.valueOf(utc.toLocalDateTime).getTime / 1000.0
  }
}
