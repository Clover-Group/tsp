package ru.itclover.tsp

import cats.syntax.either._
import com.typesafe.scalalogging.Logger
import org.apache.flink.api.common.io.RichInputFormat
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.core.io.InputSplit
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.types.Row
import org.influxdb.dto.QueryResult
import ru.itclover.tsp.core.io.{Extractor, TimeExtractor}
import ru.itclover.tsp.io.input.{InfluxDBInputConf, InfluxDBInputFormat, InputConf, JDBCInputConf}
import ru.itclover.tsp.services.{InfluxDBService, JdbcService}
import ru.itclover.tsp.utils.ErrorsADT._
import ru.itclover.tsp.utils.RowOps.{RowIdxExtractor, RowIsoTimeExtractor, RowTsTimeExtractor}

import scala.collection.JavaConverters._
import scala.collection.mutable
import ru.itclover.tsp.io.input.{KafkaInputConf}

/*sealed*/
trait StreamSource[Event, EKey, EItem] extends Product with Serializable {
  def createStream: DataStream[Event]

  def conf: InputConf[Event, EKey, EItem]

  def emptyEvent: Event

  def fieldsClasses: Seq[(Symbol, Class[_])]

  def fieldToEKey: Symbol => EKey

  def partitioner: Event => String

  implicit def timeExtractor: TimeExtractor[Event]

  implicit def extractor: Extractor[Event, EKey, EItem]
}

object StreamSource {

  def findNullField(allFields: Seq[Symbol], excludedFields: Seq[Symbol]) = {
    allFields.find { field =>
      !excludedFields.contains(field)
    }
  }
}

object JdbcSource {

  def create(conf: JDBCInputConf)(implicit strEnv: StreamExecutionEnvironment): Either[ConfigErr, JdbcSource] =
    for {
      types <- JdbcService
        .fetchFieldsTypesInfo(conf.driverName, conf.jdbcUrl, conf.query)
        .toEither
        .leftMap[ConfigErr](e => SourceUnavailable(Option(e.getMessage).getOrElse(e.toString)))
      source <- StreamSource.findNullField(types.map(_._1), conf.datetimeField +: conf.partitionFields) match {
        case Some(nullField) => JdbcSource(conf, types, nullField).asRight
        case None            => InvalidRequest("Source should contain at least one non partition and datatime field.").asLeft
      }
    } yield source
}

// todo rm nullField and trailing nulls in queries at platform (uniting now done on Flink) after states fix
case class JdbcSource(conf: JDBCInputConf, fieldsClasses: Seq[(Symbol, Class[_])], nullFieldId: Symbol)(
  implicit @transient streamEnv: StreamExecutionEnvironment
) extends StreamSource[Row, Int, Any] {

  import conf._

  val stageName = "JDBC input processing stage"
  val log = Logger[JdbcSource]
  val fieldsIdx = fieldsClasses.map(_._1).zipWithIndex
  val fieldsIdxMap = fieldsIdx.toMap
  val partitionsIdx = partitionFields.map(fieldsIdxMap)

  require(fieldsIdxMap.get(datetimeField).isDefined, "Cannot find datetime field, index overflow.")
  require(fieldsIdxMap(datetimeField) < fieldsIdxMap.size, "Cannot find datetime field, index overflow.")
  private val badPartitions = partitionFields
    .map(fieldsIdxMap.get)
    .find(idx => idx.getOrElse(Int.MaxValue) >= fieldsIdxMap.size)
    .flatten
    .map(p => fieldsClasses(p)._1)
  require(badPartitions.isEmpty, s"Cannot find partition field (${badPartitions.getOrElse('unknown)}), index overflow.")

  val timeIndex = fieldsIdxMap(datetimeField)
  val fieldsTypesInfo: Array[TypeInformation[_]] = fieldsClasses.map(c => TypeInformation.of(c._2)).toArray
  val rowTypesInfo = new RowTypeInfo(fieldsTypesInfo, fieldsClasses.map(_._1.toString.tail).toArray)

  val emptyEvent = {
    val r = new Row(fieldsIdx.length)
    fieldsIdx.foreach { case (_, ind) => r.setField(ind, 0) }
    r
  }

  override def createStream = {
    val stream = streamEnv
      .createInput(inputFormat)
      .name(stageName)
    parallelism match {
      case Some(p) => stream.setParallelism(p)
      case None    => stream
    }
  }

  def nullEvent = {
    val r = new Row(fieldsIdxMap.size)
    fieldsIdxMap.foreach { case (_, ind) => r.setField(ind, 0) }
    r
  }

  override def fieldToEKey = { fieldId: Symbol =>
    fieldsIdxMap(fieldId)
  }

  override def partitioner = {
    val serializablePI = partitionsIdx
    event: Row => serializablePI.map(event.getField).mkString
  }

  val tsMultiplier = timestampMultiplier.getOrElse {
    log.info("timestampMultiplier in JDBC source conf is not provided, use default = 1000.0")
    1000.0
  }
  override def timeExtractor = RowTsTimeExtractor(timeIndex, tsMultiplier, datetimeField)
  override def extractor = RowIdxExtractor()

  val inputFormat: RichInputFormat[Row, InputSplit] =
    JDBCInputFormatProps
      .buildJDBCInputFormat()
      .setDrivername(driverName)
      .setDBUrl(jdbcUrl)
      .setUsername(userName.getOrElse(""))
      .setPassword(password.getOrElse(""))
      .setQuery(query)
      .setRowTypeInfo(rowTypesInfo)
      .finish()
}

object InfluxDBSource {

  def create(conf: InfluxDBInputConf)(implicit strEnv: StreamExecutionEnvironment): Either[ConfigErr, InfluxDBSource] =
    for {
      types <- InfluxDBService
        .fetchFieldsTypesInfo(conf.query, conf.influxConf)
        .toEither
        .leftMap[ConfigErr](e => SourceUnavailable(Option(e.getMessage).getOrElse(e.toString)))
      source <- StreamSource.findNullField(types.map(_._1), conf.datetimeField +: conf.partitionFields) match {
        case Some(nullField) => InfluxDBSource(conf, types, nullField).asRight
        case None            => InvalidRequest("Source should contain at least one non partition and datatime field.").asLeft
      }
    } yield source
}

case class InfluxDBSource(conf: InfluxDBInputConf, fieldsClasses: Seq[(Symbol, Class[_])], nullFieldId: Symbol)(
  implicit @transient streamEnv: StreamExecutionEnvironment
) extends StreamSource[Row, Int, Any] {

  import conf._

  val dummyResult: Class[QueryResult.Result] = new QueryResult.Result().getClass.asInstanceOf[Class[QueryResult.Result]]
  val queryResultTypeInfo: TypeInformation[QueryResult.Result] = TypeInformation.of(dummyResult)
  val stageName = "InfluxDB input processing stage"
  val defaultTimeoutSec = 200L

  val fieldsIdx = fieldsClasses.map(_._1).zipWithIndex
  val fieldsIdxMap = fieldsIdx.toMap
  val partitionsIdx = partitionFields.map(fieldsIdxMap)

  require(fieldsIdxMap.get(datetimeField).isDefined, "Cannot find datetime field, index overflow.")
  require(fieldsIdxMap(datetimeField) < fieldsIdxMap.size, "Cannot find datetime field, index overflow.")
  private val badPartitions = partitionFields
    .map(fieldsIdxMap.get)
    .find(idx => idx.getOrElse(Int.MaxValue) >= fieldsIdxMap.size)
    .flatten
    .map(p => fieldsClasses(p)._1)
  require(badPartitions.isEmpty, s"Cannot find partition field (${badPartitions.getOrElse('unknown)}), index overflow.")

  val timeIndex = fieldsIdxMap(datetimeField)
  val fieldsTypesInfo: Array[TypeInformation[_]] = fieldsClasses.map(c => TypeInformation.of(c._2)).toArray
  val rowTypesInfo = new RowTypeInfo(fieldsTypesInfo, fieldsClasses.map(_._1.toString.tail).toArray)

  val emptyEvent = {
    val r = new Row(fieldsIdx.length)
    fieldsIdx.foreach { case (_, ind) => r.setField(ind, 0) }
    r
  }

  override def createStream = {
    val serFieldsIdxMap = fieldsIdxMap // for task serialization
    val stream = streamEnv
      .createInput(inputFormat)(queryResultTypeInfo)
      .flatMap(queryResult => {
        // extract Flink.rows form series of points
        if (queryResult == null || queryResult.getSeries == null) {
          mutable.Buffer[Row]()
        } else
          for {
            series   <- queryResult.getSeries.asScala
            valueSet <- series.getValues.asScala.map(_.asScala)
            if valueSet != null
          } yield {
            val tags = if (series.getTags != null) series.getTags.asScala else Map.empty
            val row = new Row(tags.size + valueSet.size)
            val fieldsAndValues = tags ++ series.getColumns.asScala.zip(valueSet)
            fieldsAndValues.foreach {
              case (field, value) => row.setField(serFieldsIdxMap(Symbol(field)), value)
            }
            row
          }
      })
      .name(stageName)
    parallelism match {
      case Some(p) => stream.setParallelism(p)
      case None    => stream
    }
  }

  override def fieldToEKey = (fieldId: Symbol) => fieldsIdxMap(fieldId)

  override def partitioner = {
    val serializablePI = partitionsIdx
    event: Row => serializablePI.map(event.getField).mkString
  }

  override def timeExtractor = RowIsoTimeExtractor(timeIndex, datetimeField)
  override def extractor = RowIdxExtractor()

  val inputFormat =
    InfluxDBInputFormat
      .create()
      .url(url)
      .timeoutSec(timeoutSec.getOrElse(defaultTimeoutSec))
      .username(userName.getOrElse(""))
      .password(password.getOrElse(""))
      .database(dbName)
      .query(query)
      .and()
      .buildIt()
}

object KafkaSource {

  def create(conf: JDBCInputConf)(implicit strEnv: StreamExecutionEnvironment): Either[ConfigErr, JdbcSource] = ???

}

case class KafkaSource(conf: KafkaInputConf, fieldsClasses: Seq[(Symbol, Class[_])], nullFieldId: Symbol)(
  implicit @transient streamEnv: StreamExecutionEnvironment
) extends StreamSource[Row, Int, Any] {

  def createStream: org.apache.flink.streaming.api.scala.DataStream[org.apache.flink.types.Row] = ???
  def emptyEvent: org.apache.flink.types.Row = ???
  implicit def extractor: ru.itclover.tsp.core.io.Extractor[org.apache.flink.types.Row, Int, Any] = ???
  def fieldToEKey: Symbol => Int = ???
  def partitioner: org.apache.flink.types.Row => String = ???
  implicit def timeExtractor: ru.itclover.tsp.core.io.TimeExtractor[org.apache.flink.types.Row] = ???

}
