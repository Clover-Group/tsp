package ru.itclover.tsp.streaming

import cats.effect.{Blocker, IO}
import doobie.implicits.{toDoobieStreamOps, toSqlInterpolator}
import doobie.util.query.Query0
import doobie.{ConnectionIO, ExecutionContexts, Transactor}
import ru.itclover.tsp.core.Pattern.IdxExtractor
import ru.itclover.tsp.core.io.{Decoder, Extractor, TimeExtractor}
import ru.itclover.tsp.streaming.io.{InputConf, JDBCInputConf, NarrowDataUnfolding, WideDataFilling}
import ru.itclover.tsp.streaming.utils.{EventCreator, KeyCreator, KeyCreatorInstances, Row, RowWithIdx}
import ru.itclover.tsp.streaming.utils.RowImplicits._
import fs2.Stream


trait StreamSource[Event, EKey, EItem, EventSchema, StreamType] extends Product with Serializable {

  def createStream: StreamType

  def conf: InputConf[Event, EKey, EItem]

  def fieldsClasses: Seq[(Symbol, Class[_])]

  def fieldToEKey: Symbol => EKey

  def fieldsIdxMap: Map[Symbol, Int]

  def transformedFieldsIdxMap: Map[Symbol, Int]

  def partitioner: Seq[String]

  def transformedPartitioner: Seq[String]

  def eventSchema: EventSchema

  def transformedEventSchema: EventSchema

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

  implicit def eventCreator: EventCreator[Event, EKey, EventSchema]

  implicit def keyCreator: KeyCreator[EKey]
}

trait FS2StreamSource[Event, EKey, EItem, EventSchema]
  extends StreamSource[Event, EKey, EItem, EventSchema, Stream[IO, Event]] {
}

case class JdbcSource(
                       conf: JDBCInputConf[RowWithIdx],
                       fieldsClasses: Seq[(Symbol, Class[_])],
                       patternFields: Set[Symbol]
                     ) extends FS2StreamSource[RowWithIdx, Symbol, Any, Map[Symbol, Class[_]]] {
  implicit val cs = IO.contextShift(ExecutionContexts.synchronous)

  val xa = Transactor.fromDriverManager[IO](
    conf.driverName,
    conf.jdbcUrl,
    conf.userName.getOrElse(""),
    conf.password.getOrElse(""),
    Blocker.liftExecutionContext(ExecutionContexts.synchronous) // just for testing
  )

  override def createStream: Stream[IO, RowWithIdx] = Query0[Row](conf.query)
    .stream
    .scan((0L, null.asInstanceOf[Row]))((rowWithIdx, newRow) => (rowWithIdx._1 + 1, newRow))
    .transact(xa)

  override def fieldToEKey: Symbol => Symbol = identity

  override def fieldsIdxMap: Map[Symbol, Int] = fieldsClasses.map(_._1).zipWithIndex.toMap

  override def transformedFieldsIdxMap: Map[Symbol, Int] = conf.dataTransformation match {
    case Some(_) =>
      val acc = transformers.SparseRowsDataAccumulator[RowWithIdx, Symbol, Any, RowWithIdx, Map[Symbol, Class[_]]](this, patternFields)(
        timeExtractor,
        kvExtractor,
        extractor,
        eventCreator,
        keyCreator,
        ??? //rowWithIdxEmptyChecker
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

  override def eventSchema: Map[Symbol, Class[_]] = ???

  override def transformedEventSchema: Map[Symbol, Class[_]] = ???

  override implicit def timeExtractor: TimeExtractor[RowWithIdx] = ???

  override implicit def transformedTimeExtractor: TimeExtractor[RowWithIdx] = ???

  override implicit def idxExtractor: IdxExtractor[RowWithIdx] = ???

  override implicit def extractor: Extractor[RowWithIdx, Symbol, Any] = ???

  override implicit def transformedExtractor: Extractor[RowWithIdx, Symbol, Any] = ???

  override implicit def itemToKeyDecoder: Decoder[Any, Symbol] = (x: Any) => Symbol(x.toString)

  override implicit def eventCreator: EventCreator[RowWithIdx, Symbol, Map[Symbol, Class[_]]] = ???

  override implicit def keyCreator: KeyCreator[Symbol] = KeyCreatorInstances.symbolKeyCreator
}

object StreamSource {

  def findNullField(allFields: Seq[Symbol], excludedFields: Seq[Symbol]) =
    allFields.find { field =>
      !excludedFields.contains(field)
    }

  // todo: no vars
  var sparkMaster: String = "local"
}