package ru.itclover.tsp.mappers

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import java.sql.Timestamp
import java.time.{Instant, LocalDateTime, ZoneId, ZoneOffset, ZonedDateTime}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.types.Row
import ru.itclover.tsp.core.{Incident, Time}
import ru.itclover.tsp.io.output._

import scala.util.Try

/**
  * Packer of found incident into [[org.apache.flink.types.Row]]
  */
case class PatternsToRowMapper[Event, EKey](sourceId: Int, schema: EventSchema) extends RichMapFunction[Incident, Row] {


  override def map(incident: Incident) = schema match {
    case newRowSchema: NewRowSchema =>
      val resultRow = new Row(newRowSchema.fieldsCount)
      newRowSchema.data.foreach {
        case (k, v) =>
          val pos = newRowSchema.fieldsIndices(Symbol(k))
          v match {
            case value: IntESValue => resultRow.setField(pos, convertFromInt(value.value, value.`type`))
            case value: FloatESValue => resultRow.setField(pos, convertFromFloat(value.value, value.`type`))
            case value: StringESValue => resultRow.setField(pos,
              convertFromString(interpolateString(value.value, incident),
                value.`type`))
            case value: ObjectESValue => resultRow.setField(pos, mapToJson(convertFromObject(value.value, incident)))
        }
      }
      resultRow
  }

  def mapToJson(data: Map[String, Any]): String = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.writeValueAsString(data)
  }

  def nowInUtcMillis: Double = {
    val zonedDt = ZonedDateTime.of(LocalDateTime.now, ZoneId.systemDefault)
    val utc = zonedDt.withZoneSameInstant(ZoneId.of("UTC"))
    Timestamp.valueOf(utc.toLocalDateTime).getTime / 1000.0
  }

  def escape(raw: String): String = {
    import scala.reflect.runtime.universe._
    Literal(Constant(raw)).toString
  }

  def toJsonString(ctx: Map[Symbol, String]): String = {
    ctx.map { case (k, v) => s"""${escape(k.name)}: ${escape(v)}""" }
      .mkString("{", ", ", "}")
  }

  def interpolateString(value: String, incident: Incident): String = {
    val replaced = value
      .replace("$IncidentID", incident.id)
      .replace("$UUID", incident.incidentUUID.toString)
      .replace("$PatternID", incident.patternId.toString)
      .replace("$Unit", incident.patternUnit.toString)
      .replace("$Subunit", incident.patternSubunit.toString)
      .replace("$IncidentStart", incident.segment.from.toString)
      .replace("$IncidentEnd", incident.segment.to.toString)
      .replace("$ProcessingDate", Time(nowInUtcMillis.toLong).toString)
      .replace("$$", "$")

    // Replace pattern metadata
    incident.patternMetadata.foldLeft(replaced) {
      case (r, (k, v)) => r.replace(s"$$PatternMetadata@$k", v)
    }
  }

  def convertFromInt(value: Long, toType: String): Any = {
    toType match {
      case "int8" => value.toByte
      case "int16" => value.toShort
      case "int32" => value.toInt
      case "int64" => value
      case "boolean" => value != 0
      case "string" => value.toString
      case "float32" => value.toFloat
      case "float64" => value.toDouble
      case "timestamp" => Timestamp.from(Instant.ofEpochSecond(value))
      case "object" => value
      case _       =>
    }
  }

  def convertFromFloat(value: Double, toType: String): Any = {
    toType match {
      case "int8" => value.toByte
      case "int16" => value.toShort
      case "int32" => value.toInt
      case "int64" => value.toLong
      case "boolean" => value != 0
      case "string" => value.toString
      case "float32" => value.toFloat
      case "float64" => value
      case "timestamp" => Timestamp.from(Instant.ofEpochSecond(value.toLong, ((value - value.toLong) * 1e9).toInt))
      case "object" => value
      case _       =>
    }
  }

  def convertFromString(value: String, toType: String): Any = {
    toType match {
      case "int8" => value.toByte
      case "int16" => value.toShort
      case "int32" => value.toInt
      case "int64" => value.toLong
      case "boolean" => value != "0" && value != "false" && value != "off"
      case "string" => value
      case "float32" => value.toFloat
      case "float64" => value.toDouble
      case "timestamp" => Timestamp.valueOf(value)
      case "object" => value
      case _       =>
    }
  }

  def convertFromObject(value: Map[String, EventSchemaValue], incident: Incident): Map[String, Any] = {
    value.map {
      case (k, v) =>
        val s = v match {
          case value: IntESValue => convertFromInt(value.value, value.`type`)
          case value: FloatESValue => convertFromFloat(value.value, value.`type`)
          case value: StringESValue => convertFromString(interpolateString(value.value, incident), value.`type`)
          case value: ObjectESValue => convertFromObject(value.value, incident)
        }
        (k, s)
    }
  }
}
