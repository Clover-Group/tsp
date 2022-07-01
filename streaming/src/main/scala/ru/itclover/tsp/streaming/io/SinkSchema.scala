package ru.itclover.tsp.streaming.io

import java.sql.Types
import java.time.{LocalDateTime, ZonedDateTime}

import scala.collection.mutable

/**
 * Schema for writing data to sink.
 */
trait SinkSchema extends Serializable {
  def rowSchema: EventSchema
}

//case class KafkaSegmentsSink(schemaUri: String, brokerList: String, topicId: String, rowSchema: RowSchema) {
//  override def toString: String = {
//    "{" + super.toString + s", fieldsIndexesMap=${rowSchema.fieldsIndexesMap}"
//  }
//}

trait EventSchema { // TODO fieldsTypesInfo to PatternsSearchJob
  def fieldsTypes: List[Int]

  def fieldsNames: List[Symbol]

  def fieldsCount: Int

  def fieldsIndices: Map[Symbol, Int]
}

sealed trait EventSchemaValue{
  def `type`: String
}

case class IntESValue(override val `type`: String, value: Long) extends EventSchemaValue
case class FloatESValue(override val `type`: String, value: Double) extends EventSchemaValue
case class StringESValue(override val `type`: String, value: String) extends EventSchemaValue
case class ObjectESValue(override val `type`: String, value: Map[String, EventSchemaValue]) extends EventSchemaValue

case class NewRowSchema(data: Map[String, EventSchemaValue]) extends EventSchema {
  override def fieldsTypes: List[Int] = data.map {
    case (_, v) => v.`type` match {
      case "int8" => Types.INTEGER
      case "int16" => Types.INTEGER
      case "int32" => Types.INTEGER
      case "int64" => Types.INTEGER
      case "boolean" => Types.BOOLEAN
      case "string" => Types.VARCHAR
      case "float32" => Types.FLOAT
      case "float64" => Types.DOUBLE
      case "timestamp" => Types.TIMESTAMP
      case "object" => Types.VARCHAR
      case _       => Types.BINARY
    }
  }.toList

  override def fieldsNames: List[Symbol] = data.map {
    case (k, _) => Symbol(k)
  }.toList

  override def fieldsCount: Int = data.size

  override def fieldsIndices: Map[Symbol, Int] = fieldsNames.zipWithIndex.toMap
}
