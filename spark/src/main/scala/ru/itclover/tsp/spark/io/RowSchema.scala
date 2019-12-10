package ru.itclover.tsp.spark.io

import org.apache.spark.sql.types.{DataType, DataTypes}

import scala.collection.mutable

case class RowSchema(
                      sourceIdField: Symbol,
                      fromTsField: Symbol,
                      toTsField: Symbol,
                      appIdFieldVal: (Symbol, Int),
                      patternIdField: Symbol,
                      processingTsField: Symbol,
                      contextField: Symbol,
                      forwardedFields: Seq[Symbol] = List.empty
                    ) extends Serializable {
  val fieldsCount: Int = 7

  val fieldsNames: List[Symbol] =
    List(sourceIdField, fromTsField, toTsField, appIdFieldVal._1, patternIdField, processingTsField, contextField)

  val fieldsIndexesMap: mutable.LinkedHashMap[Symbol, Int] = mutable.LinkedHashMap(fieldsNames.zipWithIndex: _*)


  val fieldClasses: List[Class[_]] =
    List(classOf[Int], classOf[Double], classOf[Double], classOf[Int], classOf[String], classOf[Double], classOf[String])

  val fieldDatatypes: List[DataType] =
    List(DataTypes.IntegerType, DataTypes.DoubleType, DataTypes.DoubleType, DataTypes.IntegerType,
      DataTypes.StringType, DataTypes.DoubleType, DataTypes.StringType)

  val sourceIdInd = fieldsIndexesMap(sourceIdField)

  val beginInd = fieldsIndexesMap(fromTsField)
  val endInd = fieldsIndexesMap(toTsField)

  val patternIdInd = fieldsIndexesMap(patternIdField)
  val patternPayloadInd = fieldsIndexesMap(patternIdField)

  val processingTimeInd = fieldsIndexesMap(processingTsField)

  val appIdInd = fieldsIndexesMap(appIdFieldVal._1)

  val contextInd = fieldsIndexesMap(contextField)
}