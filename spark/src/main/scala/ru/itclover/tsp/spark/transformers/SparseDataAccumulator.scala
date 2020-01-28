package ru.itclover.tsp.spark.transformers

import com.typesafe.scalalogging.Logger
import ru.itclover.tsp.core.Pattern.Idx
import ru.itclover.tsp.spark.StreamSource
import ru.itclover.tsp.core.io.{Extractor, TimeExtractor}
import ru.itclover.tsp.spark.utils.KeyCreator
import ru.itclover.tsp.core.Time
//import ru.itclover.tsp.core.Time.TimeNonTransformedExtractor
import ru.itclover.tsp.spark.utils.EventCreator
import ru.itclover.tsp.spark.io.{NarrowDataUnfolding, WideDataFilling}
import ru.itclover.tsp.core.io.AnyDecodersInstances.decodeToAny

import scala.collection.mutable
import scala.util.{Success, Try}

trait SparseDataAccumulator

/**
  * Accumulates sparse key-value format into dense Row using timeouts.
  * @param fieldsKeysTimeoutsMs - indexes to collect and timeouts (milliseconds) per each (collect by-hand for now)
  * @param extraFieldNames - will be added to every emitting event
  */
class SparseRowsDataAccumulator[InEvent, InKey, Value, OutEvent](
  fieldsKeysTimeoutsMs: Map[InKey, Long],
  extraFieldNames: Seq[InKey],
  useUnfolding: Boolean,
  defaultTimeout: Option[Long]
)(
  implicit extractTime: TimeExtractor[InEvent],
  extractKeyAndVal: InEvent => (InKey, Value),
  extractValue: Extractor[InEvent, InKey, Value],
  eventCreator: EventCreator[OutEvent, InKey],
  keyCreator: KeyCreator[InKey]
) extends Serializable {
  // potential event values with receive time
  val event: mutable.Map[InKey, (Value, Time)] = mutable.Map.empty
  val targetKeySet: Set[InKey] = fieldsKeysTimeoutsMs.keySet
  val keysIndexesMap: Map[InKey, Int] = targetKeySet.zip(0 until targetKeySet.size).toMap

  val extraFieldsIndexesMap: Map[InKey, Int] = extraFieldNames
    .filter(!keysIndexesMap.contains(_))
    .zip(
      targetKeySet.size until
      targetKeySet.size + extraFieldNames.size
    )
    .toMap
  val allFieldsIndexesMap: Map[InKey, Int] = keysIndexesMap ++ extraFieldsIndexesMap
  val arity: Int = fieldsKeysTimeoutsMs.size + extraFieldNames.size

  val log = Logger("SparseDataAccumulator")

  var lastTimestamp = Time(Long.MinValue)
  var lastEvent: OutEvent = _
  // private var lastTimer: Long = _

  def process(item: InEvent): Seq[OutEvent] = {
    val time = extractTime(item)
    if (useUnfolding) {
      val (key, value) = extractKeyAndVal(item)
      if (event.get(key).orNull == null || value != null) event(key) = (value, time)
    } else {
      allFieldsIndexesMap.keySet.foreach { key =>
        val newValue = Try(extractValue(item, key))
        newValue match {
          case Success(nv) if nv != null || !event.contains(key) => event(key) = (nv.asInstanceOf[Value], time)
          case _ => // do nothing
        }
      }
    }
    dropExpiredKeys(event, time)
    val list = mutable.ListBuffer.tabulate[(InKey, AnyRef)](arity)(x => (keyCreator.create(s"empty_$x"), null))
    val indexesMap = if (defaultTimeout.isDefined) allFieldsIndexesMap else keysIndexesMap
    event.foreach {
      case (k, (v, _)) if indexesMap.contains(k) => list(indexesMap(k)) = (k, v.asInstanceOf[AnyRef])
      case _                                     =>
    }
    //if (defaultTimeout.isEmpty) {
    extraFieldNames.foreach { name =>
      val value = extractValue(item, name)
      if (value != null) list(extraFieldsIndexesMap(name)) = (name, value.asInstanceOf[AnyRef])
    }
    //}
    val outEvent = eventCreator.create(list)
    val res = if (lastTimestamp.toMillis != time.toMillis && lastEvent != null) {
      Seq(lastEvent)
    } else {
      Seq.empty
    }
    lastTimestamp = time
    lastEvent = outEvent
    res
  }

  private def dropExpiredKeys(event: mutable.Map[InKey, (Value, Time)], currentRowTime: Time): Unit = {
    event.retain(
      (k, v) => currentRowTime.toMillis - v._2.toMillis < fieldsKeysTimeoutsMs.getOrElse(k, defaultTimeout.getOrElse(0L))
    )
  }

}

object SparseRowsDataAccumulator {

  def apply[InEvent, InKey, Value, OutEvent](
    streamSource: StreamSource[InEvent, InKey, Value],
    patternFields: Set[InKey]
  )(
    implicit timeExtractor: TimeExtractor[InEvent],
    extractKeyVal: InEvent => (InKey, Value),
    extractAny: Extractor[InEvent, InKey, Value],
    eventCreator: EventCreator[OutEvent, InKey],
    keyCreator: KeyCreator[InKey]
  ): SparseRowsDataAccumulator[InEvent, InKey, Value, OutEvent] = {
    streamSource.conf.dataTransformation
      .map({
        case ndu: NarrowDataUnfolding[InEvent, InKey, _] =>
          val sparseRowsConf = ndu
          val fim = streamSource.fieldsIdxMap
          val timeouts = patternFields
              .map(k => (k, ndu.defaultTimeout.getOrElse(0L)))
              .toMap[InKey, Long] ++
            ndu.fieldsTimeoutsMs
          val extraFields = fim
            .filterNot {
              case (name, _) => name == sparseRowsConf.keyColumn || name == sparseRowsConf.defaultValueColumn
            }
            .keys
            .toSeq
          new SparseRowsDataAccumulator(
            timeouts,
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
        case wdf: WideDataFilling[InEvent, InKey, _] =>
          val sparseRowsConf = wdf
          val fim = streamSource.fieldsIdxMap
          val toKey = streamSource.fieldToEKey
          val timeouts = patternFields
              .map(k => (k, wdf.defaultTimeout.getOrElse(0L)))
              .toMap[InKey, Long] ++
            wdf.fieldsTimeoutsMs
          val extraFields =
            fim
              .filterNot {
                case (name, _) => sparseRowsConf.fieldsTimeoutsMs.contains(toKey(name))
              }
              .keys
              .toSeq
          new SparseRowsDataAccumulator(
            timeouts,
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
}
