package ru.itclover.streammachine.transformers

import java.util
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.core.io.InputSplit
import collection.JavaConversions._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import ru.itclover.streammachine.io.input.{InfluxDBInputConf, InputConf, JDBCInputConf, RawPattern}
import ru.itclover.streammachine.utils.CollectionsOps.{RightBiasedEither, TryOps}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.types.Row
import org.influxdb.dto.QueryResult
import scala.collection.mutable


trait StreamSource[Event] {
  def createStream: Either[Throwable, DataStream[Event]]

  def inputConf: InputConf[Event]

  def emptyEvent: Event

  def getTerminalCheck: Either[Throwable, Event => Boolean]

  protected def findNullField(allFields: Seq[Symbol], excludedFields: Seq[Symbol]) = {
    allFields.find {
      field => !excludedFields.contains(field)
    } match {
      case Some(nullField) => Right(nullField)
      case None =>
        Left(new IllegalArgumentException(s"Fail to compute nullIndex, query contains only date and partition cols."))
    }
  }
}

case class JdbcSource(inputConf: JDBCInputConf)
                     (implicit streamEnv: StreamExecutionEnvironment) extends StreamSource[Row] {
  val stageName = "JDBC input processing stage"

  override def createStream = for {
    fTypesInfo <- inputConf.fieldsTypesInfo
  } yield {
    val stream = streamEnv
      .createInput(inputConf.getInputFormat(fTypesInfo.toArray))
      .name(stageName)
    inputConf.parallelism match {
      case Some(p) => stream.setParallelism(p)
      case None => stream
    }
  }

  override def emptyEvent = new Row(0)

  override def getTerminalCheck = for {
    fieldsIdxMap <- inputConf.errOrFieldsIdxMap
    nullField <- findNullField(fieldsIdxMap.keys.toSeq, inputConf.datetimeField +: inputConf.partitionFields)
    nullInd = fieldsIdxMap(nullField)
  } yield (event: Row) => event.getArity > nullInd && event.getField(nullInd) == null
}


case class InfluxDBSource(inputConf: InfluxDBInputConf)
                         (implicit streamEnv: StreamExecutionEnvironment) extends StreamSource[Row] {
  val dummyResult: Class[QueryResult.Result] = new QueryResult.Result().getClass.asInstanceOf[Class[QueryResult.Result]]
  val queryResultTypeInfo: TypeInformation[QueryResult.Result] = TypeInformation.of(dummyResult)
  val stageName = "InfluxDB input processing stage"

  override def createStream = for {
    fieldsTypesInfo <- inputConf.fieldsTypesInfo
    fieldsIdxMap <- inputConf.errOrFieldsIdxMap
  } yield {
    streamEnv
      .createInput(inputConf.getInputFormat(fieldsTypesInfo.toArray))(queryResultTypeInfo)
      .flatMap(queryResult => {
        // extract Flink.rows form series of points
        if (queryResult == null || queryResult.getSeries == null) {
          mutable.Buffer[Row]()
        }
        else for {
          series <- queryResult.getSeries
          valueSet <- series.getValues
        } yield {
          val tags = if (series.getTags != null) series.getTags else new util.HashMap[String, String]()
          val row = new Row(tags.size() + valueSet.size())
          val fieldsAndValues = tags ++ series.getColumns.toSeq.zip(valueSet)
          fieldsAndValues.foreach {
            case (field, value) => row.setField(fieldsIdxMap(Symbol(field)), value)
          }
          row
        }
      })
      .name(stageName)
  }

  override def emptyEvent = new Row(0)

  override def getTerminalCheck = for {
    fieldsIdxMap <- inputConf.errOrFieldsIdxMap
    nullField <- findNullField(fieldsIdxMap.keys.toSeq, inputConf.datetimeField +: inputConf.partitionFields)
    nullInd = fieldsIdxMap(nullField)
  } yield (event: Row) => event.getArity > nullInd && event.getField(nullInd) == null
}
