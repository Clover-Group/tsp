package ru.itclover.tsp.spark

import cats.syntax.either._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import ru.itclover.tsp.core.Pattern.IdxExtractor
import ru.itclover.tsp.core.io.{Decoder, Extractor, TimeExtractor}
import ru.itclover.tsp.spark.io.{InputConf, JDBCInputConf, NarrowDataUnfolding, WideDataFilling}
import ru.itclover.tsp.spark.transformers.SparseRowsDataAccumulator
import ru.itclover.tsp.spark.utils.ErrorsADT.{ConfigErr, InvalidRequest, SourceUnavailable}
import ru.itclover.tsp.spark.utils.{EventCreator, EventCreatorInstances, JdbcService, KeyCreator, KeyCreatorInstances, RowWithIdx}
//import ru.itclover.tsp.spark.utils.EncoderInstances._
import ru.itclover.tsp.spark.utils.RowOps.{RowSymbolExtractor, RowTsTimeExtractor}

trait StreamSource[Event, EKey, EItem] extends Product with Serializable {
  def spark: SparkSession

  def createStream: RDD[Event]

  def conf: InputConf[Event, EKey, EItem]

  def fieldsClasses: Seq[(Symbol, Class[_])]

  def fieldToEKey: Symbol => EKey

  def fieldsIdxMap: Map[Symbol, Int]

  def transformedFieldsIdxMap: Map[Symbol, Int]

  def partitioner: Event => String

  def transformedPartitioner: Event => String

  implicit def timeExtractor: TimeExtractor[Event]

  implicit def transformedTimeExtractor: TimeExtractor[Event]

  implicit def idxExtractor: IdxExtractor[Event]

  implicit def extractor: Extractor[Event, EKey, EItem]

  implicit def transformedExtractor: Extractor[Event, EKey, EItem]

  implicit def trivialEItemDecoder: Decoder[EItem, EItem] = (v1: EItem) => v1

  implicit def itemToKeyDecoder: Decoder[EItem, EKey] // for narrow data widening

  implicit def kvExtractor: Event => (EKey, EItem) = conf.dataTransformation match {
    case Some(NarrowDataUnfolding(key, value, _, _, _)) =>
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

  def create(conf: JDBCInputConf, fields: Set[Symbol]): Either[ConfigErr, JdbcSource] =
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
                     ) extends StreamSource[RowWithIdx, Symbol, Any] {
  def partitionsIdx: Seq[Int] = conf.partitionFields.filter(fieldsIdxMap.contains).map(fieldsIdxMap)
  def transformedPartitionsIdx: Seq[Int] = conf.partitionFields.map(transformedFieldsIdxMap)

  // TODO: Better place for Spark session
  override val spark: SparkSession = SparkSession.builder()
    .master("local")
    .appName("JDBC SparkSession")
    .config("spark.io.compression.codec", "snappy")
    .getOrCreate()

  override def createStream: RDD[RowWithIdx] = {
    spark.read
      .format("jdbc")
      .option("url", conf.jdbcUrl)
      .option("dbtable", s"(${conf.query})")
      .option("user", conf.userName.getOrElse(""))
      .option("password", conf.password.getOrElse(""))
      .load()
      .rdd
      .zipWithIndex()
      .map{ case (x, i) => RowWithIdx(i + 1, x) }
  }

  override def fieldToEKey: Symbol => Symbol = identity

  override def fieldsIdxMap: Map[Symbol, Int] = fieldsClasses.map(_._1).zipWithIndex.toMap

  override def transformedFieldsIdxMap: Map[Symbol, Int] = conf.dataTransformation match {
    case Some(_) =>
      val acc = SparseRowsDataAccumulator[RowWithIdx, Symbol, Any, RowWithIdx](this, patternFields)(
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

  override def partitioner: RowWithIdx => String = {
    val serializablePI = partitionsIdx
    event: RowWithIdx => serializablePI.map(event.row.get).mkString
  }

  override def transformedPartitioner: RowWithIdx => String = {
    val serializablePI = transformedPartitionsIdx
    event: RowWithIdx => serializablePI.map(event.row.get).mkString
  }

  val timeIndex = fieldsIdxMap(conf.datetimeField)
  val transformedTimeIndex = transformedFieldsIdxMap(conf.datetimeField)

  def tsMultiplier = conf.timestampMultiplier.getOrElse {
    // log.trace("timestampMultiplier in JDBC source conf is not provided, use default = 1000.0")
    1000.0
  }

  override implicit def timeExtractor: TimeExtractor[RowWithIdx] = {
    val rowExtractor = RowTsTimeExtractor(timeIndex, tsMultiplier, conf.datetimeField)
    TimeExtractor.of((r: RowWithIdx) => rowExtractor(r.row))
  }

  override implicit def transformedTimeExtractor: TimeExtractor[RowWithIdx] =
    RowTsTimeExtractor(transformedTimeIndex, tsMultiplier, conf.datetimeField).comap(_.row)

  override implicit def idxExtractor: IdxExtractor[RowWithIdx] = IdxExtractor.of(_.idx)

  override implicit def extractor: Extractor[RowWithIdx, Symbol, Any] = RowSymbolExtractor(fieldsIdxMap).comap(_.row)

  override implicit def transformedExtractor: Extractor[RowWithIdx, Symbol, Any] = RowSymbolExtractor(transformedFieldsIdxMap).comap(_.row)

  override implicit def itemToKeyDecoder: Decoder[Any, Symbol] = (x: Any) => Symbol(x.toString)

  implicit override def eventCreator: EventCreator[RowWithIdx, Symbol] =
    EventCreatorInstances.rowWithIdxSymbolEventCreator

  implicit override def keyCreator: KeyCreator[Symbol] = KeyCreatorInstances.symbolKeyCreator
}