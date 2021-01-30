package ru.itclover.tsp.io.output

import java.sql.Types
import java.time.{LocalDateTime, ZonedDateTime}

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo

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
}

case class NewRowSchema(
  unitIdField: Symbol,
  fromTsField: Symbol,
  toTsField: Symbol,
  appIdFieldVal: (Symbol, Int),
  patternIdField: Symbol,
  subunitIdField: Symbol
) extends EventSchema
    with Serializable {
  override def fieldsTypes: List[Int] =
    List(Types.INTEGER, Types.TIMESTAMP, Types.TIMESTAMP, Types.INTEGER, Types.INTEGER, Types.INTEGER)

  override def fieldsNames: List[Symbol] =
    List(unitIdField, fromTsField, toTsField, appIdFieldVal._1, patternIdField, subunitIdField)

  override def fieldsCount: Int = 6

  val fieldsIndexesMap: mutable.LinkedHashMap[Symbol, Int] = mutable.LinkedHashMap(fieldsNames.zipWithIndex: _*)

  val unitIdInd = fieldsIndexesMap(unitIdField)
  val subunitIdInd = fieldsIndexesMap(subunitIdField)

  val beginInd = fieldsIndexesMap(fromTsField)
  val endInd = fieldsIndexesMap(toTsField)

  val appIdInd = fieldsIndexesMap(appIdFieldVal._1)
  val patternIdInd = fieldsIndexesMap(patternIdField)

}
