package ru.itclover.streammachine.io.output

import java.sql.{Timestamp, Types}
import scala.collection.mutable


/**
  * Schema for writing data to sink.
  */
trait SinkSchema


/**
  * Specific schema for rules segments for PG at Clover Platform.
  */
case class PGSegmentsSink(tableName: String, sourceIdFieldVal: (Symbol, Int), beginField: Symbol, endField: Symbol,
                          appIdField: Symbol, patternIdField: Symbol, processingTimeField: Symbol, contextField: Symbol,
                          forwardedFields: Seq[Symbol] = List.empty)
    extends SinkSchema {
  val fieldsCount: Int = 7

  val fieldsNames: List[Symbol] = List(sourceIdFieldVal._1, beginField, endField, appIdField, patternIdField,
    processingTimeField, contextField)

  val fieldsIndexesMap: mutable.LinkedHashMap[Symbol, Int] = mutable.LinkedHashMap(fieldsNames.zipWithIndex:_*)

  // TODO(r): to SinkInfo with select limit 1
  val fieldTypes: List[Int] = List(Types.INTEGER, Types.DOUBLE, Types.DOUBLE, Types.INTEGER, Types.VARCHAR,
    Types.DOUBLE, Types.VARCHAR)

  val sourceIdInd = fieldsIndexesMap(sourceIdFieldVal._1)
  val sourceId = sourceIdFieldVal._2

  val beginInd = fieldsIndexesMap(beginField)
  val endInd = fieldsIndexesMap(endField)

  val patternIdInd = fieldsIndexesMap(patternIdField)
  val patternPayloadInd = fieldsIndexesMap(patternIdField)

  val processingTimeInd = fieldsIndexesMap(processingTimeField)

  val appIdInd = fieldsIndexesMap(appIdField)
  val appId = 1

  val contextInd = fieldsIndexesMap(contextField)

  /*import java.text.DateFormat
  import java.text.SimpleDateFormat

  val dateString = "03/23/2018 11:12:17.186417"
  val dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss.SSSSSS")
  val date = dateFormat.parse(dateString)
  val unixTime = date.getTime / 1000
  System.out.println(unixTime)*/


  override def toString: String = {
    "{" + super.toString + s", fieldsIndexesMap=$fieldsIndexesMap}"
  }
}
