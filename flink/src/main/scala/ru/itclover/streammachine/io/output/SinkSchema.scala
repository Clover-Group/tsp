package ru.itclover.streammachine.io.output

import java.sql.Types

import scala.collection.mutable

/**
  * Schema for writing data to sink.
  */
trait SinkSchema

/**
  * Specific schema for rules segments for Clover Platform.
  * @param forwardedFields - fields that be forwarded from source to sink
  */
case class JDBCSegmentsSink(tableName: String, fromTimeField: Symbol, fromTimeMillisField: Symbol,
                            toTimeField: Symbol, toTimeMillisField: Symbol,
                            patternIdField: Symbol, forwardedFields: Seq[Symbol] = List.empty)
    extends SinkSchema {
  val fieldsCount: Int = forwardedFields.length + 5

  val fieldsNames: List[Symbol] = List(fromTimeField, fromTimeMillisField, toTimeField, toTimeMillisField, patternIdField) ++
    forwardedFields

  val fieldsIndexesMap: mutable.LinkedHashMap[Symbol, Int] = mutable.LinkedHashMap(fieldsNames.zipWithIndex:_*)

  // TODO(r): to SinkInfo with select limit 1
  val fieldTypes: List[Int] = List(Types.VARCHAR, Types.INTEGER, Types.VARCHAR, Types.INTEGER, Types.VARCHAR) ++
    forwardedFields.map(_ => Types.LONGVARCHAR)

  val fromTimeInd = fieldsIndexesMap(fromTimeField)
  val fromTimeMillisInd = fieldsIndexesMap(fromTimeMillisField)
  val toTimeInd = fieldsIndexesMap(toTimeField)
  val toTimeMillisInd = fieldsIndexesMap(toTimeMillisField)
  val patternIdInd = fieldsIndexesMap(patternIdField)

  override def toString: String = {
    "{" + super.toString + s", fieldsIndexesMap=$fieldsIndexesMap}"
  }
}
