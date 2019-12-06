package ru.itclover.tsp.spark

import org.apache.spark.sql.Dataset
import org.apache.spark.streaming.dstream.DStream
import ru.itclover.tsp.core.Pattern.IdxExtractor
import ru.itclover.tsp.core.io.{Decoder, Extractor, TimeExtractor}
import ru.itclover.tsp.spark.io.{InputConf, JDBCInputConf}
import ru.itclover.tsp.spark.utils.ErrorsADT.{ConfigErr, InvalidRequest, SourceUnavailable}
import ru.itclover.tsp.spark.utils.{JdbcService, RowWithIdx}

trait StreamSource[Event, EKey, EItem] extends Product with Serializable {
  def createStream: Dataset[Event]

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

//  implicit def kvExtractor: Event => (EKey, EItem) = conf.dataTransformation match {
//    case Some(NarrowDataUnfolding(key, value, _, _)) =>
//      (r: Event) => (extractor.apply[EKey](r, key), extractor.apply[EItem](r, value)) // TODO: See that place better
//    case Some(WideDataFilling(_, _)) =>
//      (_: Event) => sys.error("Wide data filling does not need K-V extractor")
//    case Some(_) =>
//      (_: Event) => sys.error("Unsupported data transformation")
//    case None =>
//      (_: Event) => sys.error("No K-V extractor without data transformation")
//  }
//
//  implicit def eventCreator: EventCreator[Event, EKey]
//
//  implicit def keyCreator: KeyCreator[EKey]
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
  override def createStream: Dataset[RowWithIdx] = ???

  override def fieldToEKey: Symbol => Symbol = ???

  override def fieldsIdxMap: Map[Symbol, Int] = ???

  override def transformedFieldsIdxMap: Map[Symbol, Int] = ???

  override def partitioner: RowWithIdx => String = ???

  override def transformedPartitioner: RowWithIdx => String = ???

  override implicit def timeExtractor: TimeExtractor[RowWithIdx] = ???

  override implicit def transformedTimeExtractor: TimeExtractor[RowWithIdx] = ???

  override implicit def idxExtractor: IdxExtractor[RowWithIdx] = ???

  override implicit def extractor: Extractor[RowWithIdx, Symbol, Any] = ???

  override implicit def transformedExtractor: Extractor[RowWithIdx, Symbol, Any] = ???

  override implicit def itemToKeyDecoder: Decoder[Any, Symbol] = ???
}