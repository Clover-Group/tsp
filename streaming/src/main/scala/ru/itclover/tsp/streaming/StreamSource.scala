package ru.itclover.tsp

import cats.effect.IO
import cats.implicits._
import com.typesafe.scalalogging.Logger
import doobie.implicits._

import java.sql.{PreparedStatement, ResultSet}
import doobie.{ConnectionIO, FC, FPS, FRS, PreparedStatementIO, ResultSetIO, Transactor}
import doobie.util.stream.repeatEvalChunks
import fs2.kafka.{AutoOffsetReset, ConsumerSettings, Deserializer, KafkaConsumer}
import ru.itclover.tsp.StreamSource.Row
import ru.itclover.tsp.core.Pattern.{Idx, IdxExtractor}
import ru.itclover.tsp.core.io.{Decoder, Extractor, TimeExtractor}
import ru.itclover.tsp.services.KafkaService
import ru.itclover.tsp.streaming.io._
import ru.itclover.tsp.streaming.serialization.JsonDeserializer
import ru.itclover.tsp.streaming.services.JdbcService
import ru.itclover.tsp.streaming.transformers.SparseRowsDataAccumulator
import ru.itclover.tsp.streaming.utils.{EventCreator, EventCreatorInstances, KeyCreator, KeyCreatorInstances}
import ru.itclover.tsp.streaming.utils.ErrorsADT._
import ru.itclover.tsp.streaming.utils.RowOps.{RowSymbolExtractor, RowTsTimeExtractor}

import scala.collection.JavaConverters._
import scala.collection.mutable

// Fields types are only known at runtime, so we have to use Any here
@SuppressWarnings(Array("org.wartremover.warts.Any"))
trait StreamSource[Event, EKey, EItem] extends Product with Serializable {
  def createStream: fs2.Stream[IO, Event]

  def conf: InputConf[Event, EKey, EItem]

  def fieldsClasses: Seq[(Symbol, Class[_])]

  def transformedFieldsClasses: Seq[(Symbol, Class[_])] = conf.dataTransformation match {
    case Some(NarrowDataUnfolding(key, value, _, mapping, _)) =>
      val m: Map[EKey, List[EKey]] = mapping.getOrElse(Map.empty)
      val r = fieldsClasses ++ m.map {
        case (col, list) =>
          list.map(
            k =>
              (
                eKeyToField(k),
                fieldsClasses
                  .find {
                    case (s, _) => fieldToEKey(s) == col
                  }
                  .map(_._2)
                  .getOrElse(defaultClass)
              )
          )
      }.flatten
      r
    case _ =>
      fieldsClasses
  }

  def defaultClass: Class[_] = conf.dataTransformation match {
    case Some(NarrowDataUnfolding(_, value, _, _, _)) =>
      fieldsClasses.find { case (s, _) => fieldToEKey(s) == value }.map(_._2).getOrElse(classOf[Double])
    case _ =>
      classOf[Double]
  }

  def fieldToEKey: Symbol => EKey

  def eKeyToField: EKey => Symbol

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
    case Some(NarrowDataUnfolding(key, value, _, mapping, _)) =>
      (r: Event) =>
        // TODO: Maybe optimise that by using intermediate (non-serialised) dictionary
        val extractedKey = extractor.apply[EKey](r, key)
        val valueColumn = mapping
          .getOrElse(Map.empty[EKey, List[EKey]])
          .toSeq
          .find {
            case (_, list) =>
              list.contains(extractedKey)
          }
          .map(_._1)
          .getOrElse(value)
        val extractedValue = extractor.apply[EItem](r, valueColumn)
        (extractedKey, extractedValue) // TODO: See that place better
    case Some(WideDataFilling(_, _)) =>
      (_: Event) => sys.error("Wide data filling does not need K-V extractor")
    case Some(_) =>
      (_: Event) => sys.error("Unsupported data transformation")
    case None =>
      (_: Event) => sys.error("No K-V extractor without data transformation")
  }

  implicit def eventCreator: EventCreator[Event, EKey]

  implicit def keyCreator: KeyCreator[EKey]

  def patternFields: Set[EKey]
}

object StreamSource {
  type Row = Array[AnyRef]

  def findNullField(allFields: Seq[Symbol], excludedFields: Seq[Symbol]) =
    allFields.find { field =>
      !excludedFields.contains(field)
    }
}

case class RowWithIdx(idx: Idx, row: Row)

// Fields types are only known at runtime, so we have to use Any here
@SuppressWarnings(Array("org.wartremover.warts.Any"))
object JdbcSource {

  def create(conf: JDBCInputConf, fields: Set[Symbol]): Either[Err, JdbcSource] =
    for {
      types <- JdbcService
        .fetchFieldsTypesInfo(conf.driverName, conf.jdbcUrl, conf.query)
        .toEither
        .leftMap[ConfigErr](e => SourceUnavailable(Option(e.getMessage).getOrElse(e.toString)))
      newFields <- checkKeysExistence(conf, fields)
      source <- StreamSource.findNullField(types.map(_._1), conf.datetimeField +: conf.partitionFields) match {
        case Some(nullField) => JdbcSource(conf, types, nullField, newFields).asRight
        case None            => InvalidRequest("Source should contain at least one non partition and datatime field.").asLeft
      }
    } yield source

  def checkKeysExistence(conf: JDBCInputConf, keys: Set[Symbol]): Either[GenericRuntimeErr, Set[Symbol]] =
    conf.dataTransformation match {
      case Some(NarrowDataUnfolding(keyColumn, _, _, _, _)) =>
        JdbcService
          .fetchAvailableKeys(conf.driverName, conf.jdbcUrl, conf.query, keyColumn)
          .toEither
          .map(_.intersect(keys))
          .leftMap[GenericRuntimeErr](e => GenericRuntimeErr(e, 5099))
      case _ => Right(keys)
    }
}

// todo rm nullField and trailing nulls in queries at platform (uniting now done on Flink) after states fix
// Fields types are only known at runtime, so we have to use Any here
@SuppressWarnings(Array("org.wartremover.warts.Any"))
case class JdbcSource(
                       conf: JDBCInputConf,
                       fieldsClasses: Seq[(Symbol, Class[_])],
                       nullFieldId: Symbol,
                       patternFields: Set[Symbol]
                     ) extends StreamSource[RowWithIdx, Symbol, Any] {

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

  lazy val transactor = Transactor.fromDriverManager[IO](
    conf.driverName,
    conf.jdbcUrl,
    conf.userName.getOrElse(""),
    conf.password.getOrElse("")
  )

  def getNextChunk(chunkSize: Int): ResultSetIO[Seq[Row]] =
    FRS.raw { rs =>
      val md = rs.getMetaData
      val ks = (1 to md.getColumnCount).map(md.getColumnLabel).toList
      var n = chunkSize
      val b = Vector.newBuilder[Row]
      while (n > 0 && rs.next) {
        val vb = Array.newBuilder[AnyRef]
        ks.foreach(k => vb += rs.getObject(k))
        b += vb.result()
        n -= 1
      }
      b.result()
    }

  def liftProcessGeneric(
                          chunkSize: Int,
                          create: ConnectionIO[PreparedStatement],
                          prep:   PreparedStatementIO[Unit],
                          exec:   PreparedStatementIO[ResultSet]): fs2.Stream[ConnectionIO, Row] = {

    def prepared(ps: PreparedStatement): fs2.Stream[ConnectionIO, PreparedStatement] =
      fs2.Stream.eval[ConnectionIO, PreparedStatement] {
        val fs = FPS.setFetchSize(chunkSize)
        FC.embed(ps, fs *> prep).map(_ => ps)
      }

    def unrolled(rs: ResultSet): fs2.Stream[ConnectionIO, Row] =
      repeatEvalChunks(FC.embed(rs, getNextChunk(chunkSize)))

    val preparedStatement: fs2.Stream[ConnectionIO, PreparedStatement] =
      fs2.Stream.bracket(create)(FC.embed(_, FPS.close)).flatMap(prepared)

    def results(ps: PreparedStatement): fs2.Stream[ConnectionIO, Row] =
      fs2.Stream.bracket(FC.embed(ps, exec))(FC.embed(_, FRS.close)).flatMap(unrolled)

    preparedStatement.flatMap(results)

  }

  override def createStream: fs2.Stream[IO, RowWithIdx] =
    liftProcessGeneric(1000,
      FC.prepareStatement(conf.query),
      ().pure[PreparedStatementIO](cats.free.Free.catsFreeMonadForFree),
      FPS.executeQuery)
      .zipWithIndex
      .map { case (r, i) => RowWithIdx(i + 1, r) }
      .transact(transactor)


  override def fieldToEKey = { fieldId: Symbol =>
    fieldId
    // fieldsIdxMap(fieldId)
  }

  override def eKeyToField: Symbol => Symbol = { key: Symbol =>
    key
  }

  override def partitioner: RowWithIdx => String = {
    val serializablePI = partitionsIdx
    event: RowWithIdx => serializablePI.map(event.row.apply).mkString
  }

  override def transformedPartitioner: RowWithIdx => String = {
    val serializablePI = transformedPartitionsIdx
    event: RowWithIdx => serializablePI.map(event.row.apply).mkString
  }

  def tsMultiplier = timestampMultiplier.getOrElse {
    log.trace("timestampMultiplier in JDBC source conf is not provided, use default = 1000.0")
    1000.0
  }

  override def timeExtractor: TimeExtractor[RowWithIdx] = {
    val rowExtractor = RowTsTimeExtractor(timeIndex, tsMultiplier, datetimeField)
    TimeExtractor.of(r => rowExtractor(r.row))
  }
  override def extractor = RowSymbolExtractor(fieldsIdxMap).comap(_.row)

  override def transformedExtractor = RowSymbolExtractor(transformedFieldsIdxMap).comap(_.row)

  implicit override def eventCreator: EventCreator[RowWithIdx, Symbol] =
    EventCreatorInstances.rowWithIdxSymbolEventCreator

  implicit override def keyCreator: KeyCreator[Symbol] = KeyCreatorInstances.symbolKeyCreator

  implicit override def itemToKeyDecoder: Decoder[Any, Symbol] = (x: Any) => Symbol(x.toString)

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

  implicit override def transformedTimeExtractor: TimeExtractor[RowWithIdx] =
    RowTsTimeExtractor(transformedTimeIndex, tsMultiplier, datetimeField).comap(_.row)

  //todo refactor everything related to idxExtractor
  implicit override def idxExtractor: IdxExtractor[RowWithIdx] = IdxExtractor.of(_.idx)
}

// Fields types are only known at runtime, so we have to use Any here
@SuppressWarnings(Array("org.wartremover.warts.Any"))
object KafkaSource {

  val log = Logger[KafkaSource]

  def create(conf: KafkaInputConf, fields: Set[Symbol]): Either[ConfigErr, KafkaSource] =
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

// Fields types are only known at runtime, so we have to use Any here
@SuppressWarnings(Array("org.wartremover.warts.Any"))
case class KafkaSource(
                        conf: KafkaInputConf,
                        fieldsClasses: Seq[(Symbol, Class[_])],
                        nullFieldId: Symbol,
                        patternFields: Set[Symbol]
                      ) extends StreamSource[RowWithIdx, Symbol, Any] {

  val log = Logger[KafkaSource]

  def fieldsIdx = fieldsClasses.map(_._1).zipWithIndex
  def fieldsIdxMap = fieldsIdx.toMap

  override def fieldToEKey: Symbol => Symbol = (x => x)

  override def eKeyToField: Symbol => Symbol = (key: Symbol) => key

  def timeIndex = fieldsIdxMap(conf.datetimeField)

  def tsMultiplier = conf.timestampMultiplier.getOrElse {
    log.debug("timestampMultiplier in Kafka source conf is not provided, use default = 1000.0")
    1000.0
  }

  implicit def extractor: ru.itclover.tsp.core.io.Extractor[RowWithIdx, Symbol, Any] =
    RowSymbolExtractor(fieldsIdxMap).comap(_.row)
  implicit def timeExtractor: ru.itclover.tsp.core.io.TimeExtractor[RowWithIdx] =
    RowTsTimeExtractor(timeIndex, tsMultiplier, conf.datetimeField).comap(_.row)

  val stageName = "Kafka input processing stage"

  val consumerSettings = ConsumerSettings(
    keyDeserializer = Deserializer.unit[IO],
    valueDeserializer = Deserializer.instance[IO, Row](
      (topic, headers, bytes) => {
        val deserialized = JsonDeserializer(fieldsClasses).deserialize(bytes)
        deserialized match {
          case Right(value) => IO.pure(value)
          case Left(throwable) => ??? // TODO: Deserialization error
        }
      }
    )
  )
    .withBootstrapServers(conf.brokers)
    .withGroupId(conf.group)
    .withAutoOffsetReset(AutoOffsetReset.Latest)

  def createStream: fs2.Stream[IO, RowWithIdx] =
    KafkaConsumer
      .stream(consumerSettings)
      .subscribeTo(conf.topic)
      .records
      .map { committable =>
        committable.record.value
      }
      .zipWithIndex
      .map { case (r, i) => RowWithIdx(i + 1, r) }

  def partitionsIdx = conf.partitionFields.filter(fieldsIdxMap.contains).map(fieldsIdxMap)
  def transformedPartitionsIdx = conf.partitionFields.map(transformedFieldsIdxMap)

  def partitioner = {
    val serializablePI = partitionsIdx
    event: RowWithIdx => serializablePI.map(event.row.apply).mkString
  }

  def transformedPartitioner = {
    val serializablePI = transformedPartitionsIdx
    event: RowWithIdx => serializablePI.map(event.row.apply).mkString
  }

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
    RowTsTimeExtractor(transformedTimeIndex, tsMultiplier, conf.datetimeField).comap(_.row)

  implicit override def transformedExtractor: Extractor[RowWithIdx, Symbol, Any] =
    RowSymbolExtractor(transformedFieldsIdxMap).comap(_.row)

  implicit override def itemToKeyDecoder: Decoder[Any, Symbol] = (x: Any) => Symbol(x.toString)

  implicit override def eventCreator: EventCreator[RowWithIdx, Symbol] =
    EventCreatorInstances.rowWithIdxSymbolEventCreator

  implicit override def keyCreator: KeyCreator[Symbol] = KeyCreatorInstances.symbolKeyCreator
  //todo refactor everything related to idxExtractor
  implicit override def idxExtractor: IdxExtractor[RowWithIdx] = IdxExtractor.of(_.idx)

}
