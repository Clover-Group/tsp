package ru.itclover.tsp.streaming.mappers

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import ru.itclover.tsp.StreamSource.Row

import java.sql.Timestamp
import java.time.{Instant, LocalDateTime, ZoneId, ZonedDateTime}
import ru.itclover.tsp.core.{Incident, Time}
import ru.itclover.tsp.streaming.io._

import spray.json._
import DefaultJsonProtocol._
import scala.util.Try

/** Packer of found incident into [[Row]]
  */
case class PatternsToRowMapper[Event, EKey](schema: EventSchema) {

  def map(incident: Incident) = schema match {
    case newRowSchema: NewRowSchema =>
      val resultRow = new Row(newRowSchema.fieldsCount)
      newRowSchema.data.foreach { case (k, v) =>
        val pos = newRowSchema.fieldsIndices(String(k))
        v match {
          case value: IntESValue   => resultRow(pos) = convertFromInt(value.value, value.`type`).asInstanceOf[AnyRef]
          case value: FloatESValue => resultRow(pos) = convertFromFloat(value.value, value.`type`).asInstanceOf[AnyRef]
          case value: StringESValue =>
            resultRow(pos) =
              convertFromString(interpolateString(value.value, incident), value.`type`).asInstanceOf[AnyRef]
          case value: ObjectESValue => resultRow(pos) = mapToJson(convertFromObject(value.value, incident))
        }
      }
      resultRow
  }

  def mapToJson(data: Map[String, Any]): String = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.writeValueAsString(data)
  }

  def nowInUtcMillis: Long = {
    val zonedDt = ZonedDateTime.of(LocalDateTime.now, ZoneId.systemDefault)
    val utc = zonedDt.withZoneSameInstant(ZoneId.of("UTC"))
    Timestamp.valueOf(utc.toLocalDateTime).getTime
  }

  def toJsonString(ctx: Map[String, String]): String = {
    ctx.toJson.toString()
  }

  def interpolateString(value: String, incident: Incident): String = {
    val replaced = value
      .replace("$IncidentID", incident.id)
      .replace("$UUID", incident.incidentUUID.toString)
      .replace("$PatternID", incident.patternId.toString)
      .replace("$Unit", incident.patternUnit.toString)
      .replace("$Subunit", incident.patternSubunit.toString)
      .replace("$IncidentUnixMsStart", incident.segment.from.toMillis.toString)
      .replace("$IncidentUnixMsEnd", incident.segment.to.toMillis.toString)
      .replace("$IncidentStart", incident.segment.from.toString)
      .replace("$IncidentEnd", incident.segment.to.toString)
      .replace("$ProcessingDate", Time(nowInUtcMillis).toString)
      .replace("$$", "$")

    // Replace pattern metadata
    val replacedMetadataKeys = incident.patternMetadata.foldLeft(replaced) { case (r, (k, v)) =>
      r.replace(s"$$PatternMetadata@$k", v)
    }

    // Replace partition fields values
    val replacedPFV = incident.partitionFieldsValues.foldLeft(replaced) { case (r, (k, v)) =>
      r.replace(s"$$PartitionFieldsValues@$k", v)
    }

    replacedPFV
      .replace(
        "$PatternMetadataAndPartitionFieldsValues",
        (incident.patternMetadata ++ incident.partitionFieldsValues).toJson.compactPrint
      )
      .replace("$PatternMetadata", incident.patternMetadata.toJson.compactPrint)
      .replace("$PartitionFieldsValues", incident.partitionFieldsValues.toJson.compactPrint)
  }

  def convertFromInt(value: Long, toType: String): Any = {
    toType match {
      case "int8"      => value.toByte
      case "int16"     => value.toShort
      case "int32"     => value.toInt
      case "int64"     => value
      case "boolean"   => value != 0
      case "string"    => value.toString
      case "float32"   => value.toFloat
      case "float64"   => value.toDouble
      case "timestamp" => Timestamp.from(Instant.ofEpochSecond(value))
      case "object"    => value
      case _           =>
    }
  }

  def convertFromFloat(value: Double, toType: String): Any = {
    toType match {
      case "int8"      => value.toByte
      case "int16"     => value.toShort
      case "int32"     => value.toInt
      case "int64"     => value.toLong
      case "boolean"   => value != 0
      case "string"    => value.toString
      case "float32"   => value.toFloat
      case "float64"   => value
      case "timestamp" => Timestamp.from(Instant.ofEpochSecond(value.toLong, ((value - value.toLong) * 1e9).toInt))
      case "object"    => value
      case _           =>
    }
  }

  def convertFromString(value: String, toType: String): Any = {
    toType match {
      case "int8"    => value.toByte
      case "int16"   => value.toShort
      case "int32"   => value.toInt
      case "int64"   => value.toLong
      case "boolean" => value != "0" && value != "false" && value != "off"
      case "string"  => value
      case "float32" => Try(value.toFloat).orElse(Try(Timestamp.valueOf(value).getTime() / 1000.0f)).getOrElse(Float.NaN)
      case "float64" =>
        Try(value.toDouble).orElse(Try(Timestamp.valueOf(value).getTime() / 1000.0)).getOrElse(Double.NaN)
      case "timestamp" => Timestamp.valueOf(value)
      case "object"    => value
      case _           =>
    }
  }

  def convertFromObject(value: Map[String, EventSchemaValue], incident: Incident): Map[String, Any] = {
    value.map { case (k, v) =>
      val s = v match {
        case value: IntESValue    => convertFromInt(value.value, value.`type`)
        case value: FloatESValue  => convertFromFloat(value.value, value.`type`)
        case value: StringESValue => convertFromString(interpolateString(value.value, incident), value.`type`)
        case value: ObjectESValue => convertFromObject(value.value, incident)
      }
      (k, s)
    }
  }

}
