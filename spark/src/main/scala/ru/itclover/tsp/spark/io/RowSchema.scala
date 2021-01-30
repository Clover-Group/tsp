package ru.itclover.tsp.spark.io

import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.types.{DataType, DataTypes}

import scala.collection.mutable

case class NewRowSchema(
  unitIdField: Symbol,
  fromTsField: Symbol,
  toTsField: Symbol,
  appIdFieldVal: (Symbol, Int),
  patternIdField: Symbol,
  subunitIdField: Symbol
) extends Serializable {
  val fieldsCount: Int = 6

  val fieldsNames: List[Symbol] =
    List(unitIdField, fromTsField, toTsField, appIdFieldVal._1, patternIdField, subunitIdField)

  val fieldsIndexesMap: mutable.LinkedHashMap[Symbol, Int] = mutable.LinkedHashMap(fieldsNames.zipWithIndex: _*)

  val fieldClasses: List[Class[_]] =
    List(classOf[Int], classOf[Timestamp], classOf[Timestamp], classOf[Int], classOf[Int], classOf[Int])

  val fieldDatatypes: List[DataType] =
    List(
      DataTypes.IntegerType,
      DataTypes.TimestampType,
      DataTypes.TimestampType,
      DataTypes.IntegerType,
      DataTypes.IntegerType,
      DataTypes.IntegerType
    )

  val sourceIdInd = fieldsIndexesMap(unitIdField)

  val beginInd = fieldsIndexesMap(fromTsField)
  val endInd = fieldsIndexesMap(toTsField)

  val patternIdInd = fieldsIndexesMap(patternIdField)

  val appIdInd = fieldsIndexesMap(appIdFieldVal._1)

  val subunitIdInd = fieldsIndexesMap(subunitIdField)
}
