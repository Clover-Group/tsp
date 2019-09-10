package ru.itclover.tsp.transformers

import com.typesafe.scalalogging.Logger
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import ru.itclover.tsp.StreamSource
import ru.itclover.tsp.core.io.{Extractor, TimeExtractor}
import ru.itclover.tsp.utils.KeyCreator
//import ru.itclover.tsp.phases.NumericPhases.InKeyNumberExtractor
//import ru.itclover.tsp.EvalUtils
import ru.itclover.tsp.core.{Pattern, Time}
//import ru.itclover.tsp.core.Time.TimeNonTransformedExtractor
import ru.itclover.tsp.io.{EventCreator, EventCreatorInstances}
import ru.itclover.tsp.io.input.{InputConf, JDBCInputConf, NarrowDataUnfolding, WideDataFilling}
//import ru.itclover.tsp.phases.Phases.{AnyExtractor, AnyNonTransformedExtractor}
import ru.itclover.tsp.core.io.AnyDecodersInstances.decodeToAny
import ru.itclover.tsp.utils.KeyCreatorInstances._

import scala.collection.mutable
import scala.util.{Success, Try}
import scala.util.control.NonFatal

trait SparseDataAccumulator

/**
  * Accumulates sparse key-value format into dense Row using timeouts.
  * @param fieldsKeysTimeoutsMs - indexes to collect and timeouts (milliseconds) per each (collect by-hand for now)
  * @param extraFieldNames - will be added to every emitting event
  */
case class SparseRowsDataAccumulator[InEvent, Value, OutEvent](
  fieldsKeysTimeoutsMs: Map[Symbol, Long],
  extraFieldNames: Seq[Symbol],
  useUnfolding: Boolean,
  defaultTimeout: Option[Long]
)(
  implicit extractTime: TimeExtractor[InEvent],
  extractKeyAndVal: InEvent => (Symbol, Value),
  extractValue: Extractor[InEvent, Symbol, Value],
  eventCreator: EventCreator[OutEvent, Symbol],
  keyCreator: KeyCreator[Symbol]
) extends RichFlatMapFunction[InEvent, OutEvent]
    with Serializable {
  // potential event values with receive time
  val event: mutable.Map[Symbol, (Value, Time)] = mutable.Map.empty
  val targetKeySet: Set[Symbol] = fieldsKeysTimeoutsMs.keySet
  val keysIndexesMap: Map[Symbol, Int] = targetKeySet.zip(0 until targetKeySet.size).toMap

  val extraFieldsIndexesMap: Map[Symbol, Int] = extraFieldNames
    .zip(
      targetKeySet.size until
      targetKeySet.size + extraFieldNames.size
    )
    .toMap
  val allFieldsIndexesMap: Map[Symbol, Int] = keysIndexesMap ++ extraFieldsIndexesMap
  val arity: Int = fieldsKeysTimeoutsMs.size + extraFieldNames.size

  val log = Logger("SparseDataAccumulator")

  override def flatMap(item: InEvent, out: Collector[OutEvent]): Unit = {
    val time = extractTime(item)
    if (useUnfolding) {
      val (key, value) = extractKeyAndVal(item)
      event(key) = (value, time)
    } else {
      allFieldsIndexesMap.keySet.foreach { key =>
        val newValue = Try(extractValue(item, key))
        newValue match {
          case Success(nv) if nv != null || !event.contains(key) => event(key) = (nv.asInstanceOf[Value], time)
        }
      }
    }
    dropExpiredKeys(event, time)
    if (!useUnfolding || (targetKeySet subsetOf event.keySet)) {
      val list = mutable.ListBuffer.tabulate[(Symbol, AnyRef)](arity)(x => (keyCreator.create(s"empty_$x"), null))
      val indexesMap = if (defaultTimeout.isDefined) allFieldsIndexesMap else keysIndexesMap
      event.foreach {
        case (k, (v, _)) if indexesMap.contains(k) => list(indexesMap(k)) = (k, v.asInstanceOf[AnyRef])
        case _                                     =>
      }
      if (defaultTimeout.isEmpty) {
        extraFieldNames.foreach { name =>
          val value = extractValue(item, name)
          if (value != null) list(extraFieldsIndexesMap(name)) = (name, value.asInstanceOf[AnyRef])
        }
      }
      val outEvent = eventCreator.create(list)
      out.collect(outEvent)
    }
  }

  private def dropExpiredKeys(event: mutable.Map[Symbol, (Value, Time)], currentRowTime: Time): Unit = {
    event.retain(
      (k, v) => currentRowTime.toMillis - v._2.toMillis < fieldsKeysTimeoutsMs.getOrElse(k, defaultTimeout.getOrElse(0L))
    )
  }
}

object SparseRowsDataAccumulator {

  def apply[InEvent, Value, OutEvent](streamSource: StreamSource[InEvent, Symbol, Value])(
    implicit timeExtractor: TimeExtractor[InEvent],
    extractKeyVal: InEvent => (Symbol, Value),
    extractAny: Extractor[InEvent, Symbol, Value],
    rowTypeInfo: TypeInformation[OutEvent],
    eventCreator: EventCreator[OutEvent, Symbol],
    keyCreator: KeyCreator[Symbol]
  ): SparseRowsDataAccumulator[InEvent, Value, OutEvent] = {
    streamSource.conf.dataTransformation
      .map({
        case ndu: NarrowDataUnfolding[InEvent, Symbol, _] =>
          val sparseRowsConf = ndu
          val fim = streamSource.fieldsIdxMap
          val extraFields = fim
            .filterNot {
              case (name, _) => name == sparseRowsConf.keyColumn || name == sparseRowsConf.valueColumn
            }
            .keys
            .toSeq
          new SparseRowsDataAccumulator(
            sparseRowsConf.fieldsTimeoutsMs,
            extraFields.map(streamSource.fieldToEKey),
            useUnfolding = true,
            defaultTimeout = ndu.defaultTimeout
          )(
            timeExtractor,
            extractKeyVal,
            extractAny,
            eventCreator,
            keyCreator
          )
        case wdf: WideDataFilling[InEvent, Symbol, _] =>
          val sparseRowsConf = wdf
          val fim = streamSource.fieldsIdxMap
          val toKey = streamSource.fieldToEKey
          val extraFields =
            fim
              .filterNot {
                case (name, _) => sparseRowsConf.fieldsTimeoutsMs.contains(toKey(name))
              }
              .keys
              .toSeq
          new SparseRowsDataAccumulator(
            sparseRowsConf.fieldsTimeoutsMs,
            extraFields.map(streamSource.fieldToEKey),
            useUnfolding = false,
            defaultTimeout = wdf.defaultTimeout
          )(
            timeExtractor,
            extractKeyVal,
            extractAny,
            eventCreator,
            keyCreator
          )
        case _ =>
          sys.error(
            s"Invalid config type: expected NarrowDataUnfolding or WideDataFilling, got ${streamSource.conf.dataTransformation} instead"
          )
      })
      .getOrElse(sys.error("No data transformation config specified"))
  }

  def emptyEvent[InEvent](
    streamSource: StreamSource[InEvent, Symbol, Any]
  )(implicit eventCreator: EventCreator[InEvent, Symbol]): InEvent = {
    val toKey = streamSource.fieldToEKey
    eventCreator.emptyEvent(streamSource.fieldsIdxMap.map { case (k, v) => (toKey(k), v) })
  }
}
