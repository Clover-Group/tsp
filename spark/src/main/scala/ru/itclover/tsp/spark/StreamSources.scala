package ru.itclover.tsp.spark

import cats.syntax.either._
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Encoder, SparkSession, functions}
import ru.itclover.tsp.core.Pattern.IdxExtractor
import ru.itclover.tsp.core.io.{Decoder, Extractor, TimeExtractor}
import ru.itclover.tsp.spark.io.{InputConf, JDBCInputConf, KafkaInputConf, NarrowDataUnfolding, WideDataFilling}
import ru.itclover.tsp.spark.transformers.SparseRowsDataAccumulator
import ru.itclover.tsp.spark.utils.ErrorsADT.{ConfigErr, InvalidRequest, SourceUnavailable}
import ru.itclover.tsp.spark.utils.{
  EventCreator,
  EventCreatorInstances,
  JdbcService,
  KeyCreator,
  KeyCreatorInstances,
  RowWithIdx
}

import scala.util.Try
import ru.itclover.tsp.spark.utils.RowOps.{RowSymbolExtractor, RowTsTimeExtractor}

trait StreamSource[Event, EKey, EItem] extends Product with Serializable {
  def spark: SparkSession

  def createStream: Dataset[Event]

  def conf: InputConf[Event, EKey, EItem]

  def fieldsClasses: Seq[(Symbol, Class[_])]

  def fieldToEKey: Symbol => EKey

  def fieldsIdxMap: Map[Symbol, Int]

  def transformedFieldsIdxMap: Map[Symbol, Int]

  def partitioner: Seq[String]

  def transformedPartitioner: Seq[String]

  def eventSchema: StructType

  def eventEncoder: Encoder[Event]

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

  // todo: no vars
  var sparkMaster: String = "local"
}

// Stream sources deal heavily with Any values, so we must use it
@SuppressWarnings(Array("org.wartremover.warts.Any"))
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
// Stream sources deal heavily with Any values, so we must use it
@SuppressWarnings(Array("org.wartremover.warts.Any"))
case class JdbcSource(
  conf: JDBCInputConf,
  fieldsClasses: Seq[(Symbol, Class[_])],
  nullFieldId: Symbol,
  patternFields: Set[Symbol]
) extends StreamSource[RowWithIdx, Symbol, Any] {
  def partitionsIdx: Seq[Int] = conf.partitionFields.filter(fieldsIdxMap.contains).map(fieldsIdxMap)
  def transformedPartitionsIdx: Seq[Int] = conf.partitionFields.map(transformedFieldsIdxMap)

  // TODO: Better place for Spark session
  override val spark: SparkSession = SparkSession
    .builder()
    .master(StreamSource.sparkMaster)
    .appName("TSP Spark")
    .config("spark.io.compression.codec", "snappy")
    .getOrCreate()

  override val eventSchema = StructType(
    fieldsClasses.map {
      case (fieldName, typeName) =>
        StructField(
          fieldName.name,
          typeName match {
            case _: Class[Byte]    => ByteType
            case _: Class[Short]   => ShortType
            case _: Class[Int]     => IntegerType
            case _: Class[Long]    => LongType
            case _: Class[Float]   => FloatType
            case _: Class[Double]  => DoubleType
            case _: Class[Boolean] => BooleanType
            case _: Class[String]  => StringType
            case _                 => ObjectType(classOf[Any])
          }
        )
    }.toSeq
  )

  val rowEncoder = RowEncoder(eventSchema)
  override val eventEncoder: Encoder[RowWithIdx] = ExpressionEncoder.tuple(ExpressionEncoder[Long](), rowEncoder)

  override def createStream: Dataset[RowWithIdx] = {
    spark.read
      .format("jdbc")
      .option("url", conf.jdbcUrl)
      .option("dbtable", s"(${conf.query})")
      .option("user", conf.userName.getOrElse(""))
      .option("password", conf.password.getOrElse(""))
      .load()
      //.rdd
      //.zipWithIndex()
      .map { RowWithIdx(0, _) }(eventEncoder)
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

  override def partitioner: Seq[String] = {
    conf.partitionFields.filter(fieldsIdxMap.contains).map(_.name)
  }

  override def transformedPartitioner: Seq[String] = {
    conf.partitionFields.filter(transformedFieldsIdxMap.contains).map(_.name)
  }

  val timeIndex = fieldsIdxMap(conf.datetimeField)
  val transformedTimeIndex = transformedFieldsIdxMap(conf.datetimeField)

  def tsMultiplier = conf.timestampMultiplier.getOrElse {
    // log.trace("timestampMultiplier in JDBC source conf is not provided, use default = 1000.0")
    1000.0
  }

  implicit override def timeExtractor: TimeExtractor[RowWithIdx] = {
    val rowExtractor = RowTsTimeExtractor(timeIndex, tsMultiplier, conf.datetimeField)
    TimeExtractor.of((r: RowWithIdx) => rowExtractor(r._2))
  }

  implicit override def transformedTimeExtractor: TimeExtractor[RowWithIdx] =
    RowTsTimeExtractor(transformedTimeIndex, tsMultiplier, conf.datetimeField).comap(_._2)

  implicit override def idxExtractor: IdxExtractor[RowWithIdx] = IdxExtractor.of(_._1)

  implicit override def extractor: Extractor[RowWithIdx, Symbol, Any] = RowSymbolExtractor(fieldsIdxMap).comap(_._2)

  implicit override def transformedExtractor: Extractor[RowWithIdx, Symbol, Any] =
    RowSymbolExtractor(transformedFieldsIdxMap).comap(_._2)

  implicit override def itemToKeyDecoder: Decoder[Any, Symbol] = (x: Any) => Symbol(x.toString)

  implicit override def eventCreator: EventCreator[RowWithIdx, Symbol] =
    EventCreatorInstances.rowWithIdxSymbolEventCreator

  implicit override def keyCreator: KeyCreator[Symbol] = KeyCreatorInstances.symbolKeyCreator
}

// Stream sources deal heavily with Any values, so we must use it
@SuppressWarnings(Array("org.wartremover.warts.Any"))
object KafkaSource {

  def fetchFieldsTypesInfo(conf: KafkaInputConf): Try[Seq[(Symbol, Class[_])]] = Try(conf.fieldsTypes.map {
    case (fieldName, fieldType) =>
      val fieldClass = fieldType match {
        case "int8"    => classOf[Byte]
        case "int16"   => classOf[Short]
        case "int32"   => classOf[Int]
        case "int64"   => classOf[Long]
        case "float32" => classOf[Float]
        case "float64" => classOf[Double]
        case "boolean" => classOf[Boolean]
        case "string"  => classOf[String]
        case _         => classOf[Any]
      }
      (Symbol(fieldName), fieldClass)
  }.toSeq)

  def create(conf: KafkaInputConf, fields: Set[Symbol]): Either[ConfigErr, KafkaSource] =
    for {
      types <- fetchFieldsTypesInfo(conf).toEither
        .leftMap[ConfigErr](e => SourceUnavailable(Option(e.getMessage).getOrElse(e.toString)))
      source <- StreamSource.findNullField(types.map(_._1), conf.datetimeField +: conf.partitionFields) match {
        case Some(nullField) => KafkaSource(conf, types, nullField, fields).asRight
        case None            => InvalidRequest("Source should contain at least one non partition and datatime field.").asLeft
      }
    } yield source
}

// Stream sources deal heavily with Any values, so we must use it
@SuppressWarnings(Array("org.wartremover.warts.Any"))
case class KafkaSource(
  conf: KafkaInputConf,
  fieldsClasses: Seq[(Symbol, Class[_])],
  nullFieldId: Symbol,
  patternFields: Set[Symbol]
) extends StreamSource[RowWithIdx, Symbol, Any] {
  override val spark: SparkSession = SparkSession
    .builder()
    .master(StreamSource.sparkMaster)
    .appName("TSP Spark")
    .config("spark.io.compression.codec", "snappy")
    .getOrCreate()

  override val eventSchema: StructType = StructType(
    conf.fieldsTypes.map {
      case (fieldName, typeName) =>
        StructField(
          fieldName.name,
          typeName match {
            case "int8"    => ByteType
            case "int16"   => ShortType
            case "int32"   => IntegerType
            case "int64"   => LongType
            case "float32" => FloatType
            case "float64" => DoubleType
            case "boolean" => BooleanType
            case "string"  => StringType
            case _         => ObjectType(classOf[Any])
          }
        )
    }.toSeq
  )

  val rowEncoder = RowEncoder(eventSchema)
  override val eventEncoder: Encoder[RowWithIdx] =
    ExpressionEncoder.tuple(ExpressionEncoder[Long](), rowEncoder).asInstanceOf[Encoder[RowWithIdx]]

  def fieldsIdx = fieldsClasses.map(_._1).zipWithIndex
  def fieldsIdxMap = fieldsIdx.toMap

  override def fieldToEKey: Symbol => Symbol = identity

  def timeIndex = fieldsIdxMap(conf.datetimeField)

  def tsMultiplier = conf.timestampMultiplier.getOrElse {
    //log.debug("timestampMultiplier in Kafka source conf is not provided, use default = 1000.0")
    1000.0
  }

  implicit def extractor: ru.itclover.tsp.core.io.Extractor[RowWithIdx, Symbol, Any] =
    RowSymbolExtractor(fieldsIdxMap).comap(_._2)
  implicit def timeExtractor: ru.itclover.tsp.core.io.TimeExtractor[RowWithIdx] =
    RowTsTimeExtractor(timeIndex, tsMultiplier, conf.datetimeField).comap(_._2)

  val stageName = "Kafka input processing stage"

//  override val eventSchema = StructType(
//    fieldsClasses.map {
//      case (fieldName, typeName) => StructField(fieldName.name, typeName match {
//        case _: Class[Byte]    => ByteType
//        case _: Class[Short]   => ShortType
//        case _: Class[Int]     => IntegerType
//        case _: Class[Long]    => LongType
//        case _: Class[Float]   => FloatType
//        case _: Class[Double]  => DoubleType
//        case _: Class[Boolean] => BooleanType
//        case _: Class[String]  => StringType
//        case _                 => ObjectType(classOf[Any])
//      })
//  }.toSeq
//  )

  override def createStream: Dataset[RowWithIdx] = {
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", conf.brokers)
      .option("subscribe", conf.topic)
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING) as message")
      .select(functions.from_json(functions.col("message"), eventSchema).as("json"))
      .select("json.*")
      .map { RowWithIdx(0, _) }(eventEncoder)
  }

  def partitionsIdx: Seq[Int] = conf.partitionFields.filter(fieldsIdxMap.contains).map(fieldsIdxMap)
  def transformedPartitionsIdx: Seq[Int] = conf.partitionFields.map(transformedFieldsIdxMap)

  def partitioner: Seq[String] = conf.partitionFields.map(_.name)

  def transformedPartitioner: Seq[String] = conf.partitionFields.map(_.name)

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

  val transformedTimeIndex = transformedFieldsIdxMap(conf.datetimeField)

  implicit override def transformedTimeExtractor: TimeExtractor[RowWithIdx] =
    RowTsTimeExtractor(transformedTimeIndex, tsMultiplier, conf.datetimeField).comap(_._2)

  implicit override def transformedExtractor: Extractor[RowWithIdx, Symbol, Any] =
    RowSymbolExtractor(transformedFieldsIdxMap).comap(_._2)

  implicit override def itemToKeyDecoder: Decoder[Any, Symbol] = (x: Any) => Symbol(x.toString)

  implicit override def eventCreator: EventCreator[RowWithIdx, Symbol] =
    EventCreatorInstances.rowWithIdxSymbolEventCreator

  implicit override def keyCreator: KeyCreator[Symbol] = KeyCreatorInstances.symbolKeyCreator
  //todo refactor everything related to idxExtractor
  implicit override def idxExtractor: IdxExtractor[RowWithIdx] = IdxExtractor.of(_._1)
}
