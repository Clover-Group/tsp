package ru.itclover.tsp.streaming.io

import scala.collection.mutable

case class RowSchema(
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
    List(classOf[Int], classOf[Long], classOf[Long], classOf[Int], classOf[Int], classOf[Int])

  val unitIdInd = fieldsIndexesMap(unitIdField)

  val beginInd = fieldsIndexesMap(fromTsField)
  val endInd = fieldsIndexesMap(toTsField)

  val patternIdInd = fieldsIndexesMap(patternIdField)

  val appIdInd = fieldsIndexesMap(appIdFieldVal._1)

  val subunitIdInd = fieldsIndexesMap(subunitIdField)
}
