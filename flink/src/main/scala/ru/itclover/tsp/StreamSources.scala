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
import ru.itclover.tsp.core.io.{Decoder, Extractor, TimeExtractor}
import ru.itclover.tsp.io.{EventCreator, EventCreatorInstances}
import ru.itclover.tsp.io.input.{InfluxDBInputConf, InfluxDBInputFormat, InputConf, JDBCInputConf, KafkaInputConf, NarrowDataUnfolding, RedisInputConf, WideDataFilling}
import ru.itclover.tsp.services.{InfluxDBService, JdbcService, KafkaService, RedisService}
import ru.itclover.tsp.utils.ErrorsADT._
import ru.itclover.tsp.utils.{KeyCreator, KeyCreatorInstances}
import ru.itclover.tsp.utils.RowOps.{RowIsoTimeExtractor, RowSymbolExtractor, RowTsTimeExtractor}
import ru.itclover.tsp.transformers.SparseRowsDataAccumulator
import scredis.serialization.Reader

import scala.collection.JavaConverters._
import scala.util.{ Success, Failure }
import scala.collection.mutable

/*sealed*/
trait StreamSource[Event, EKey, EItem] extends Product with Serializable {
  def createStream: DataStream[Event]

  def conf: InputConf[Event, EKey, EItem]

  def emptyEvent: Event

  def fieldsClasses: Seq[(Symbol, Class[_])]

  def fieldToEKey: Symbol => EKey

  def fieldsIdxMap: Map[Symbol, Int]

  def transformedFieldsIdxMap: Map[Symbol, Int]

  def partitioner: Event => String

  def transformedPartitioner: Event => String

  implicit def timeExtractor: TimeExtractor[Event]

  implicit def transformedTimeExtractor: TimeExtractor[Event]

  implicit def extractor: Extractor[Event, EKey, EItem]

  implicit def transformedExtractor: Extractor[Event, EKey, EItem]

  implicit def trivialEItemDecoder: Decoder[EItem, EItem] = (v1: EItem) => v1

  implicit def itemToKeyDecoder: Decoder[EItem, EKey] // for narrow data widening

  implicit def kvExtractor: Event => (EKey, EItem) = conf.dataTransformation match {
    case Some(NarrowDataUnfolding(key, value, _, _)) =>
      (r: Event) => (extractor.apply[EKey](r, key), extractor.apply[EItem](r, value)) // TODO: See that place better
    case Some(WideDataFilling(_, _)) =>
      (_: Event) => sys.error("Wide data filling does not need K-V extractor")
    case Some(_) =>
      (_: Event) => sys.error("Unsupported data transformation")
    case None =>
      (_: Event) => sys.error("No K-V extractor without data transformation")
  }

  implicit def eventCreator: EventCreator[Event, EKey]

  implicit def keyCreator: KeyCreator[EKey]
}

object StreamSource {

  def findNullField(allFields: Seq[Symbol], excludedFields: Seq[Symbol]) =
    allFields.find { field =>
      !excludedFields.contains(field)
    }
}

object JdbcSource {

  def create(conf: JDBCInputConf, fields: Set[Symbol])(
    implicit strEnv: StreamExecutionEnvironment
  ): Either[ConfigErr, JdbcSource] =
    for {
      types <- JdbcService
        .fetchFieldsTypesInfo(conf.driverName, conf.jdbcUrl, conf.query)
        .toEither
        .leftMap[ConfigErr](e => SourceUnavailable(Option(e.getMessage).getOrElse(e.toString)))
      source <- StreamSource.findNullField(types.map(_._1), conf.datetimeField +: conf.partitionFields) match {
        case Some(nullField) => JdbcSource(conf, types, nullField, fields).asRight
        case None            => InvalidRequest("Source should contain at least one non partition and datatime field.").asLeft
      }
    } yield source
}

// todo rm nullField and trailing nulls in queries at platform (uniting now done on Flink) after states fix
case class JdbcSource(
  conf: JDBCInputConf,
  fieldsClasses: Seq[(Symbol, Class[_])],
  nullFieldId: Symbol,
  patternFields: Set[Symbol]
)(
  implicit @transient streamEnv: StreamExecutionEnvironment
) extends StreamSource[Row, Symbol, Any] {

  import conf._

  val stageName = "JDBC input processing stage"
  val log = Logger[JdbcSource]
  val fieldsIdx = fieldsClasses.map(_._1).zipWithIndex
  val fieldsIdxMap = fieldsIdx.toMap
  def partitionsIdx = partitionFields.filter(fieldsIdxMap.contains).map(fieldsIdxMap)
  def transformedPartitionsIdx = partitionFields.map(transformedFieldsIdxMap)

  require(fieldsIdxMap.get(datetimeField).isDefined, "Cannot find datetime field, index overflow.")
  require(fieldsIdxMap(datetimeField) < fieldsIdxMap.size, "Cannot find datetime field, index overflow.")
  private val badPartitions = partitionFields
    .map(fieldsIdxMap.get)
    .find(idx => idx.getOrElse(Int.MaxValue) >= fieldsIdxMap.size)
    .flatten
    .map(p => fieldsClasses(p)._1)
  require(badPartitions.isEmpty, s"Cannot find partition field (${badPartitions.getOrElse('unknown)}), index overflow.")

  val timeIndex = fieldsIdxMap(datetimeField)
  val transformedTimeIndex = transformedFieldsIdxMap(datetimeField)
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
    fieldId
  // fieldsIdxMap(fieldId)
  }

  override def partitioner = {
    val serializablePI = partitionsIdx
    event: Row => serializablePI.map(event.getField).mkString
  }

  override def transformedPartitioner = {
    val serializablePI = transformedPartitionsIdx
    event: Row => serializablePI.map(event.getField).mkString
  }

  def tsMultiplier = timestampMultiplier.getOrElse {
    log.info("timestampMultiplier in JDBC source conf is not provided, use default = 1000.0")
    1000.0
  }

  override def timeExtractor = RowTsTimeExtractor(timeIndex, tsMultiplier, datetimeField)
  override def extractor = RowSymbolExtractor(fieldsIdxMap)
  override def transformedExtractor = RowSymbolExtractor(transformedFieldsIdxMap)

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

  implicit override def eventCreator: EventCreator[Row, Symbol] = EventCreatorInstances.rowSymbolEventCreator

  implicit override def keyCreator: KeyCreator[Symbol] = KeyCreatorInstances.symbolKeyCreator

  implicit override def itemToKeyDecoder: Decoder[Any, Symbol] = (x: Any) => Symbol(x.toString)

  override def transformedFieldsIdxMap: Map[Symbol, Int] = conf.dataTransformation match {
    case Some(_) =>
      val acc = SparseRowsDataAccumulator[Row, Symbol, Any, Row](this, patternFields)(
        createTypeInformation[Row],
        timeExtractor,
        kvExtractor,
        extractor,
        eventCreator,
        keyCreator
      )
      acc.allFieldsIndexesMap
    case None =>
      fieldsIdxMap
  }

  implicit override def transformedTimeExtractor: TimeExtractor[Row] =
    RowTsTimeExtractor(transformedTimeIndex, tsMultiplier, datetimeField)
}

object InfluxDBSource {

  def create(conf: InfluxDBInputConf, fields: Set[Symbol])(
    implicit strEnv: StreamExecutionEnvironment
  ): Either[ConfigErr, InfluxDBSource] =
    for {
      types <- InfluxDBService
        .fetchFieldsTypesInfo(conf.query, conf.influxConf)
        .toEither
        .leftMap[ConfigErr](e => SourceUnavailable(Option(e.getMessage).getOrElse(e.toString)))
      source <- StreamSource.findNullField(types.map(_._1), conf.datetimeField +: conf.partitionFields) match {
        case Some(nullField) => InfluxDBSource(conf, types, nullField, fields).asRight
        case None            => InvalidRequest("Source should contain at least one non partition and datatime field.").asLeft
      }
    } yield source
}

case class InfluxDBSource(
  conf: InfluxDBInputConf,
  fieldsClasses: Seq[(Symbol, Class[_])],
  nullFieldId: Symbol,
  patternFields: Set[Symbol]
)(
  implicit @transient streamEnv: StreamExecutionEnvironment
) extends StreamSource[Row, Symbol, Any] {

  import conf._

  val dummyResult: Class[QueryResult.Result] = new QueryResult.Result().getClass.asInstanceOf[Class[QueryResult.Result]]
  val queryResultTypeInfo: TypeInformation[QueryResult.Result] = TypeInformation.of(dummyResult)
  val stageName = "InfluxDB input processing stage"
  val defaultTimeoutSec = 200L

  val fieldsIdx = fieldsClasses.map(_._1).zipWithIndex
  val fieldsIdxMap = fieldsIdx.toMap
  def partitionsIdx = partitionFields.filter(fieldsIdxMap.contains).map(fieldsIdxMap)
  def transformedPartitionsIdx = partitionFields.map(transformedFieldsIdxMap)

  require(fieldsIdxMap.get(datetimeField).isDefined, "Cannot find datetime field, index overflow.")
  require(fieldsIdxMap(datetimeField) < fieldsIdxMap.size, "Cannot find datetime field, index overflow.")
  private val badPartitions = partitionFields
    .map(fieldsIdxMap.get)
    .find(idx => idx.getOrElse(Int.MaxValue) >= fieldsIdxMap.size)
    .flatten
    .map(p => fieldsClasses(p)._1)
  require(badPartitions.isEmpty, s"Cannot find partition field (${badPartitions.getOrElse('unknown)}), index overflow.")

  val timeIndex = fieldsIdxMap(datetimeField)
  val transformedTimeIndex = transformedFieldsIdxMap(datetimeField)
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

  override def fieldToEKey = (fieldId: Symbol) => fieldId // fieldsIdxMap(fieldId)

  override def partitioner = {
    val serializablePI = partitionsIdx
    event: Row => serializablePI.map(event.getField).mkString
  }

  override def transformedPartitioner = {
    val serializablePI = transformedPartitionsIdx
    event: Row => serializablePI.map(event.getField).mkString
  }

  override def timeExtractor = RowIsoTimeExtractor(timeIndex, datetimeField)
  override def extractor = RowSymbolExtractor(fieldsIdxMap)
  override def transformedExtractor = RowSymbolExtractor(transformedFieldsIdxMap)

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

  implicit override def eventCreator: EventCreator[Row, Symbol] = EventCreatorInstances.rowSymbolEventCreator

  implicit override def keyCreator: KeyCreator[Symbol] = KeyCreatorInstances.symbolKeyCreator

  implicit override def itemToKeyDecoder: Decoder[Any, Symbol] = (x: Any) => Symbol(x.toString)

  override def transformedFieldsIdxMap: Map[Symbol, Int] = conf.dataTransformation match {
    case Some(_) =>
      val acc = SparseRowsDataAccumulator[Row, Symbol, Any, Row](this, patternFields)(
        createTypeInformation[Row],
        timeExtractor,
        kvExtractor,
        extractor,
        eventCreator,
        keyCreator
      )
      acc.allFieldsIndexesMap
    case None =>
      fieldsIdxMap
  }

  implicit override def transformedTimeExtractor: TimeExtractor[Row] =
    RowIsoTimeExtractor(transformedTimeIndex, datetimeField)

}

object KafkaSource {

  val log = Logger[KafkaSource]

  def create(conf: KafkaInputConf, fields: Set[Symbol])(
    implicit strEnv: StreamExecutionEnvironment
  ): Either[ConfigErr, KafkaSource] =
    for {
      types <- KafkaService
        .fetchFieldsTypesInfo(conf)
        .toEither
        .leftMap[ConfigErr](e => SourceUnavailable(Option(e.getMessage).getOrElse(e.toString)))
      _ = log.info(s"Kafka types found: $types")
      source <- StreamSource.findNullField(types.map(_._1), conf.datetimeField +: conf.partitionFields) match {
        case Some(nullField) => KafkaSource(conf, types, nullField, fields).asRight
        case None            => InvalidRequest("Source should contain at least one non partition and datatime field.").asLeft
      }
    } yield source

}

case class KafkaSource(
  conf: KafkaInputConf,
  fieldsClasses: Seq[(Symbol, Class[_])],
  nullFieldId: Symbol,
  patternFields: Set[Symbol]
)(
  implicit @transient streamEnv: StreamExecutionEnvironment
) extends StreamSource[Row, Symbol, Any] {

  val log = Logger[KafkaSource]

  def fieldsIdx = fieldsClasses.map(_._1).zipWithIndex
  def fieldsIdxMap = fieldsIdx.toMap

//  def fieldToEKey: Symbol => Int = { fieldId: Symbol =>
//    fieldsIdxMap(fieldId)
//  }

  override def fieldToEKey: Symbol => Symbol = (x => x)

  def timeIndex = fieldsIdxMap(conf.datetimeField)

  def tsMultiplier = conf.timestampMultiplier.getOrElse {
    log.info("timestampMultiplier in Kafka source conf is not provided, use default = 1000.0")
    1000.0
  }

  implicit def extractor: ru.itclover.tsp.core.io.Extractor[org.apache.flink.types.Row, Symbol, Any] =
    RowSymbolExtractor(fieldsIdxMap)
  implicit def timeExtractor: ru.itclover.tsp.core.io.TimeExtractor[org.apache.flink.types.Row] =
    RowTsTimeExtractor(timeIndex, tsMultiplier, conf.datetimeField)

  val stageName = "Kafka input processing stage"

  def createStream: DataStream[Row] = {
    val consumer = KafkaService.consumer(conf, fieldsIdxMap)
    consumer.setStartFromEarliest()
    streamEnv.enableCheckpointing(5000)
    streamEnv
      .addSource(consumer)
      .name(stageName)
    //.keyBy(_ => "nokey")
    //.process(new TimeOutFunction(5000, timeIndex, fieldsIdxMap.size))
  }

  val emptyEvent = {
    val r = new Row(fieldsIdx.length)
    fieldsIdx.foreach { case (_, ind) => r.setField(ind, 0) }
    r
  }

  def partitionsIdx = conf.partitionFields.filter(fieldsIdxMap.contains).map(fieldsIdxMap)
  def transformedPartitionsIdx = conf.partitionFields.map(transformedFieldsIdxMap)

  def partitioner = {
    val serializablePI = partitionsIdx
    event: Row => serializablePI.map(event.getField).mkString
  }

  def transformedPartitioner = {
    val serializablePI = transformedPartitionsIdx
    event: Row => serializablePI.map(event.getField).mkString
  }

  override def transformedFieldsIdxMap: Map[Symbol, Int] = conf.dataTransformation match {
    case Some(_) =>
      val acc = SparseRowsDataAccumulator[Row, Symbol, Any, Row](this, patternFields)(
        createTypeInformation[Row],
        timeExtractor,
        kvExtractor,
        extractor,
        eventCreator,
        keyCreator
      )
      acc.allFieldsIndexesMap
    case None =>
      fieldsIdxMap
  }

  val transformedTimeIndex = transformedFieldsIdxMap(conf.datetimeField)

  implicit override def transformedTimeExtractor: TimeExtractor[Row] =
    RowTsTimeExtractor(transformedTimeIndex, tsMultiplier, conf.datetimeField)

  implicit override def transformedExtractor: Extractor[Row, Symbol, Any] = RowSymbolExtractor(transformedFieldsIdxMap)

  implicit override def itemToKeyDecoder: Decoder[Any, Symbol] = (x: Any) => Symbol(x.toString)

  implicit override def eventCreator: EventCreator[Row, Symbol] = EventCreatorInstances.rowSymbolEventCreator

  implicit override def keyCreator: KeyCreator[Symbol] = KeyCreatorInstances.symbolKeyCreator
}

object RedisSource{

  val log = Logger[RedisSource]

  def create(conf: RedisInputConf, fields: Set[Symbol])(
    implicit strEnv: StreamExecutionEnvironment
  ): Either[ConfigErr, RedisSource] =
    for {
      types <- RedisService.fetchFieldsTypesInfo(conf)
          .toEither
          .leftMap[ConfigErr](e => SourceUnavailable(Option(e.getMessage).getOrElse(e.toString)))
      _ = log.info(s"Redis types found: $types")
      source <- StreamSource.findNullField(types.map(_._1), conf.datetimeField +: conf.partitionFields) match {
        case Some(nullField) => RedisSource(conf, types, nullField, fields).asRight
        case None => InvalidRequest("Source should contain at least one non partition and datetime field.").asLeft
      }
    } yield source

}

case class RedisSource(
  conf: RedisInputConf,
  fieldsClasses: Seq[(Symbol, Class[_])],
  nullFieldId: Symbol,
  patternFields: Set[Symbol]
)(
  implicit @transient streamEnv: StreamExecutionEnvironment
) extends StreamSource[Row, Symbol, Any]{

  val log: Logger = Logger[RedisSource]

  def fieldsIdx: Seq[(Symbol, Int)] = fieldsClasses.map(_._1).zipWithIndex
  override def fieldsIdxMap: Map[Symbol, Int] = fieldsIdx.toMap

  def partitionsIdx: Seq[Int] = conf.partitionFields.filter(fieldsIdxMap.contains).map(fieldsIdxMap)
  def transformedPartitionsIdx: Seq[Int] = conf.partitionFields.map(transformedFieldsIdxMap)

  def timeIndex = fieldsIdxMap(conf.datetimeField)
  def tsMultiplier = {
    log.info(s"No timestamp multiplier for Redis source")
    1000.0
  }

  val transformedTimeIndex = transformedFieldsIdxMap(conf.datetimeField)

  override def createStream: DataStream[Row] = {

    val info = conf.serializationInfo
    val rows: mutable.ListBuffer[Row] = mutable.ListBuffer.empty[Row]

    info.foreach(elem =>{

      val (client, deserializer) = RedisService.clientInstance(conf, elem)

      implicit val reader: Reader[Array[Byte]] = (bytes: Array[Byte]) => bytes

      import client.dispatcher
      client.get[Array[Byte]](elem.key).onComplete {
        case Success(value) => rows += deserializer.deserialize(value.get, fieldsIdxMap)
        case Failure(e) => throw new Exception(e.getMessage)
      }

      client.quit().value.get.get

    })

    streamEnv.fromCollection(rows)

  }

  override def emptyEvent: Row = {
    val r = new Row(fieldsIdx.length)
    fieldsIdx.foreach { case (_, ind) => r.setField(ind, 0) }
    r
  }
  override def fieldToEKey: Symbol => Symbol = (x => x)

  override def transformedFieldsIdxMap: Map[Symbol, Int] = conf.dataTransformation match {
    case Some(_) =>
      val acc = SparseRowsDataAccumulator[Row, Symbol, Any, Row](this, patternFields)(
        createTypeInformation[Row],
        timeExtractor,
        kvExtractor,
        extractor,
        eventCreator,
        keyCreator
      )
      acc.allFieldsIndexesMap
    case None =>
      fieldsIdxMap
  }

  override def partitioner: Row => String = {
    event: Row => partitionsIdx.map(event.getField).mkString
  }

  override def transformedPartitioner: Row => String = {
    event: Row => transformedPartitionsIdx.map(event.getField).mkString
  }

  override implicit def timeExtractor: TimeExtractor[Row] = RowTsTimeExtractor(timeIndex, tsMultiplier, conf.datetimeField)

  override implicit def transformedTimeExtractor: TimeExtractor[Row] = RowTsTimeExtractor(transformedTimeIndex, tsMultiplier, conf.datetimeField)

  override implicit def extractor: Extractor[Row, Symbol, Any] = RowSymbolExtractor(fieldsIdxMap)

  override implicit def transformedExtractor: Extractor[Row, Symbol, Any] = RowSymbolExtractor(transformedFieldsIdxMap)

  override implicit def itemToKeyDecoder: Decoder[Any, Symbol] = (x: Any) => Symbol(x.toString)

  override implicit def eventCreator: EventCreator[Row, Symbol] = EventCreatorInstances.rowSymbolEventCreator

  override implicit def keyCreator: KeyCreator[Symbol] = KeyCreatorInstances.symbolKeyCreator
}
