package ru.itclover.tsp.io.output

import java.sql.{Timestamp, Types}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import ru.itclover.tsp.core.Segment
import scala.collection.mutable


/**
  * Schema for writing data to sink.
  */
trait SinkSchema extends Serializable {
  def rowSchema: RowSchema
}


//case class KafkaSegmentsSink(schemaUri: String, brokerList: String, topicId: String, rowSchema: RowSchema) {
//  override def toString: String = {
//    "{" + super.toString + s", fieldsIndexesMap=${rowSchema.fieldsIndexesMap}"
//  }
//}


//trait EventSchema { // TODO fieldsTypesInfo to PatternsSearchJob
//  require(fieldsCount == fieldsTypes.length)
//
//  def fieldsTypes: List[Class[_]]
//
//  val fieldsNames: List[Symbol]
//
//  val fieldsCount: Int = fieldsNames.length
//}

/**
  * Schema, used for result row construction for sinks. Params are names of fields in sink.
  * @param appIdFieldVal - special one, tuple of name and value (Clover Platform specific)
  * @param processingTsField - time of rule processing field name
  * @param contextField - name of JSONB for Postgree or Varchar for other DBs
  * @param forwardedFields - fields that will be pushed to contextField
  */
case class RowSchema(sourceIdField: Symbol, fromTsField: Symbol, toTsField: Symbol, appIdFieldVal: (Symbol, Int),
                     patternIdField: Symbol, processingTsField: Symbol, contextField: Symbol,
                     forwardedFields: Seq[Symbol] = List.empty) extends Serializable {
  val fieldsCount: Int = 7

  val fieldsNames: List[Symbol] = List(sourceIdField, fromTsField, toTsField, appIdFieldVal._1, patternIdField,
    processingTsField, contextField)

  val fieldsIndexesMap: mutable.LinkedHashMap[Symbol, Int] = mutable.LinkedHashMap(fieldsNames.zipWithIndex:_*)

  val fieldTypes: List[Int] = List(Types.INTEGER, Types.DOUBLE, Types.DOUBLE, Types.INTEGER, Types.VARCHAR,
    Types.DOUBLE, Types.VARCHAR)

  val fieldClasses: List[Class[_]] = List(classOf[Int], classOf[Double], classOf[Double], classOf[Int], classOf[String],
    classOf[Double], classOf[String])

  val sourceIdInd = fieldsIndexesMap(sourceIdField)

  val beginInd = fieldsIndexesMap(fromTsField)
  val endInd = fieldsIndexesMap(toTsField)

  val patternIdInd = fieldsIndexesMap(patternIdField)
  val patternPayloadInd = fieldsIndexesMap(patternIdField)

  val processingTimeInd = fieldsIndexesMap(processingTsField)

  val appIdInd = fieldsIndexesMap(appIdFieldVal._1)

  val contextInd = fieldsIndexesMap(contextField)

  def getTypeInfo = new RowTypeInfo(fieldClasses.map(TypeInformation.of(_)) :_*)

  def getJdbcTypes = ??? // TODO(r): make using SinkInfo with select limit 1
}
