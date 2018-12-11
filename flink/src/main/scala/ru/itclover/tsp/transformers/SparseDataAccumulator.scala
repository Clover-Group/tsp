//package ru.itclover.tsp.transformers
//
//import org.apache.flink.streaming.api.scala.DataStream
//import ru.itclover.tsp.io._
//import ru.itclover.tsp.io.input.InputConf
//import com.typesafe.scalalogging.Logger
//import org.apache.flink.api.common.functions.RichFlatMapFunction
//import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
//import org.apache.flink.api.common.typeinfo.TypeInformation
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.shaded.netty4.io.netty.handler.codec.DecoderResultProvider
//import org.apache.flink.streaming.api.scala.DataStream
//import org.apache.flink.types.Row
//import org.apache.flink.util.Collector
//import ru.itclover.tsp.core.{Pattern, Time}
//import ru.itclover.tsp.io.input.{InputConf, NarrowDataUnfolding, WideDataFilling}
//import ru.itclover.tsp.utils.KeyCreator
//
//import scala.collection.mutable
//import scala.util.{Success, Try}
//import scala.util.control.NonFatal
//
//trait SparseDataAccumulator
//
///**
//  * Accumulates sparse key-value format into dense Row using timeouts.
//  * @param fieldsKeysTimeoutsMs - indexes to collect and timeouts (milliseconds) per each (collect by-hand for now)
//  * @param extraFieldNames - will be added to every emitting event
//  */
//case class SparseRowsDataAccumulator[InEvent, Key, Value, OutEvent](
//  fieldsKeysTimeoutsMs: Map[Key, Long],
//  extraFieldNames: Seq[Key],
//  useUnfolding: Boolean,
//  defaultTimeout: Option[Long]
//)(
//  implicit extractTime: TimeExtractor[InEvent],
//  extractKeyAndVal: InEvent => (Key, Value),
//  extractAny: Extractor[InEvent, Key, Value],
//  eventCreator: EventCreator[OutEvent, Key],
//  keyCreator: KeyCreator[Key]
//) extends RichFlatMapFunction[InEvent, OutEvent]
//    with Serializable {
//  // potential event values with receive time
//  val event: mutable.Map[Key, (Value, Time)] = mutable.Map.empty
//  val targetKeySet: Set[Key] = fieldsKeysTimeoutsMs.keySet
//  val keysIndexesMap: Map[Key, Int] = targetKeySet.zip(0 until targetKeySet.size).toMap
//
//  val extraFieldsIndexesMap: Map[Key, Int] = extraFieldNames
//    .zip(
//      targetKeySet.size until
//      targetKeySet.size + extraFieldNames.size
//    )
//    .toMap
//  val allFieldsIndexesMap: Map[Key, Int] = keysIndexesMap ++ extraFieldsIndexesMap
//  val arity: Int = fieldsKeysTimeoutsMs.size + extraFieldNames.size
//
//  val log = Logger("SparseDataAccumulator")
//
//  override def flatMap(item: InEvent, out: Collector[OutEvent]): Unit = {
//    val time = extractTime(item)
//    if (useUnfolding) {
//      val (key, value) = extractKeyAndVal(item)
//      event(key) = (value, time)
//    } else {
//      allFieldsIndexesMap.keySet.foreach { key =>
//        val newValue = Try(extractAny(item, key)((v1: Value) => v1))
//        newValue match {
//          case Success(nv) if nv != null || !event.contains(key) => event(key) = (nv.asInstanceOf[Value], time)
//          case _                                                 => ()
//        }
//      }
//    }
//    dropExpiredKeys(event, time)
//    if (!useUnfolding || (targetKeySet subsetOf event.keySet)) {
//      val list = mutable.ListBuffer.tabulate[(Key, AnyRef)](arity)(x => (keyCreator.create(s"empty_$x"), null))
//      val indexesMap = if (defaultTimeout.isDefined) allFieldsIndexesMap else keysIndexesMap
//      event.foreach {
//        case (k, (v, _)) if indexesMap.contains(k) => list(indexesMap(k)) = (k, v.asInstanceOf[AnyRef])
//        case _                                     =>
//      }
//      if (defaultTimeout.isEmpty) {
//        extraFieldNames.foreach { name =>
//          val value = extractAny[Any](item, name)
//          if (value != null) list(extraFieldsIndexesMap(name)) = (name, value.asInstanceOf[AnyRef])
//        }
//      }
//      val outEvent = eventCreator.create(list)
//      out.collect(outEvent)
//    }
//  }
//
//  private def dropExpiredKeys(event: mutable.Map[Key, (Value, Time)], currentRowTime: Time): Unit = {
//    event.retain(
//      (k, v) => currentRowTime.toMillis - v._2.toMillis < fieldsKeysTimeoutsMs.getOrElse(k, defaultTimeout.getOrElse(0L))
//    )
//  }
//}
//
//object SparseRowsDataAccumulator {
//
//  def apply[InEvent, Key, Value, OutEvent](streamSource: StreamSource[InEvent, Key, Value])(
//    rowTypeInfo: TypeInformation[OutEvent],
//    eventCreator: EventCreator[OutEvent, Key]
//  ): SparseRowsDataAccumulator[InEvent, Key, Value, OutEvent] = {
//    val inputConf = streamSource.conf
//    val extractAny = streamSource.extractor
//    val timeExtractor = streamSource.timeExtractor
//    val extractKeyVal = streamSource.conf.dataTransformation.get.kvExtractors.keyValueExtractor
//    inputConf.dataTransformation
//      .map({
//        case ndu: NarrowDataUnfolding[InEvent, Key, Value] =>
//          val sparseRowsConf = ndu
//          val fim = inputConf.errOrFieldsIdxMap match {
//            case Right(m) => m
//            case Left(e)  => sys.error(e.toString)
//          }
//          val extraFields = fim
//            .filterNot(
//              nameAndInd => nameAndInd._1 == sparseRowsConf.key || nameAndInd._1 == sparseRowsConf.value
//            )
//            .keys
//            .toSeq
//          SparseRowsDataAccumulator(
//            sparseRowsConf.fieldsTimeoutsMs,
//            extraFields,
//            useUnfolding = true,
//            defaultTimeout = ndu.defaultTimeout
//          )(
//            timeExtractor,
//            extractKeyVal,
//            extractAny,
//            eventCreator
//          )
//        case wdf: WideDataFilling =>
//          val sparseRowsConf = wdf
//          val fim = inputConf.errOrFieldsIdxMap match {
//            case Right(m) => m
//            case Left(e)  => sys.error(e.toString)
//          }
//          val extraFields =
//            fim.filterNot(nameAndInd => sparseRowsConf.fieldsTimeoutsMs.contains(nameAndInd._1)).keys.toSeq
//          SparseRowsDataAccumulator(
//            sparseRowsConf.fieldsTimeoutsMs,
//            extraFields,
//            useUnfolding = false,
//            defaultTimeout = wdf.defaultTimeout
//          )(
//            timeExtractor,
//            extractKeyVal,
//            extractAny,
//            eventCreator
//          )
//        case _ =>
//          sys.error(
//            s"Invalid config type: expected NarrowDataUnfolding or WideDataFilling, got ${inputConf.dataTransformation} instead"
//          )
//      })
//      .getOrElse(sys.error("No data transformation config specified"))
//  }
//
//  def fieldsIndexesMap[InEvent, Key, Value](inputConf: InputConf[InEvent, Key, Value]): Map[Key, Int] = {
//    val fim = inputConf.errOrFieldsIdxMap match {
//      case Right(m) => m
//      case Left(e)  => sys.error(e.toString)
//    }
//    val (extraFields, targetKeySet, useUnfolding) = inputConf.dataTransformation
//      .map({
//        case ndu: NarrowDataUnfolding =>
//          (
//            fim
//              .filterNot(
//                nameAndInd => nameAndInd._1 == ndu.key || nameAndInd._1 == ndu.value
//              )
//              .keys
//              .toSeq,
//            ndu.fieldsTimeoutsMs.keySet,
//            true
//          )
//        case wdf: WideDataFilling =>
//          (
//            fim
//              .filterNot(
//                nameAndInd => wdf.fieldsTimeoutsMs.contains(nameAndInd._1)
//              )
//              .keys
//              .toSeq,
//            wdf.fieldsTimeoutsMs.keySet,
//            false
//          )
//      })
//      .getOrElse(sys.error("Invalid config type"))
//    val keysIndexesMap: Map[Key, Int] = targetKeySet.zip(0 until targetKeySet.size).toMap
//
//    def extraFieldsIndexesMap[Key]: Map[Key, Int] = extraFields
//      .zip(
//        targetKeySet.size until
//        targetKeySet.size + extraFields.size
//      )
//      .toMap
//    // TODO: Replace that ugly workaround
//    def fieldsIndexesMap[Key]: Map[Key, Int] = keysIndexesMap ++ extraFieldsIndexesMap
//    fieldsIndexesMap
//  }
//
//  def emptyEvent[InEvent, Key, Value](inputConf: InputConf[InEvent, Key, Value])(implicit eventCreator: EventCreator[InEvent, Key]): InEvent = {
//    eventCreator.emptyEvent(fieldsIndexesMap(inputConf))
//  }
//}
//
//case class SparseDataAccumulatorSource[InEvent, OutEvent: TypeInformation, EKey, EItem](
//  originalSource: StreamSource[InEvent, EKey, EItem]
//)(implicit outEventCreator: EventCreator[OutEvent, EKey])
//    extends StreamSource[OutEvent, EKey, EItem] {
//  require(
//    originalSource.conf.dataTransformation.isDefined,
//    "Empty source data transformation: cannot create transformer for sparse data accumulator"
//  )
//
//  override def createStream: DataStream[OutEvent] = originalSource.createStream.flatMap(SparseRowsDataAccumulator(originalSource))
//  override def conf: InputConf[OutEvent, EKey, EItem] = ???
//  override def emptyEvent: OutEvent = outEventCreator.emptyEvent(???)
//  override def fieldsClasses: scala.Seq[(Symbol, Class[_])] = ???
//  override def isEventTerminal: OutEvent => Boolean = ???
//  override def fieldToEKey: Symbol => EKey = originalSource.fieldToEKey
//  override def partitioner: OutEvent => String = ???
//
//  implicit override def timeExtractor: TimeExtractor[OutEvent] = conf.dataTransformation.map { dt =>
//    dt.extractors.timeExtractor
//  }.get
//
//  implicit override def extractor: Extractor[OutEvent, EKey, EItem] = conf.dataTransformation.map { dt =>
//    dt.extractors.anyExtractor
//  }.get
//
//  private def fieldsTimeoutsMs: Map[EKey, Long] = originalSource.conf.dataTransformation match {
//    case Some(NarrowDataUnfolding(_, _, fto, _)) => fto
//    case Some(WideDataFilling(fto, _)) => fto
//    case _ => Map.empty
//  }
//}
