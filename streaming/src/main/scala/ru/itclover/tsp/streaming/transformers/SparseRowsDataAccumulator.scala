package ru.itclover.tsp.streaming.transformers

import com.typesafe.scalalogging.Logger
import ru.itclover.tsp.StreamSource
import ru.itclover.tsp.core.Time
import ru.itclover.tsp.core.io.AnyDecodersInstances.decodeToAny
import ru.itclover.tsp.core.io.{Extractor, TimeExtractor}
import ru.itclover.tsp.streaming.io.{NarrowDataUnfolding, WideDataFilling}
import ru.itclover.tsp.streaming.utils.{EventCreator, KeyCreator, EventPrinter}

import scala.collection.mutable
import scala.util.{Success, Try}
import java.util.concurrent.atomic.AtomicLong

class SparseRowsDataAccumulator[InEvent, InKey, Value, OutEvent](
  fieldsKeysTimeoutsMs: Map[InKey, Long],
  extraFieldNames: Seq[InKey],
  useUnfolding: Boolean,
  defaultTimeout: Option[Long],
  eventsMaxGapMs: Long,
  regularityInterval: Option[Long]
)(implicit
  extractTime: TimeExtractor[InEvent],
  extractKeyAndVal: InEvent => (InKey, Value),
  extractValue: Extractor[InEvent, InKey, Value],
  eventCreator: EventCreator[OutEvent, InKey],
  eventPrinter: EventPrinter[OutEvent],
  keyCreator: KeyCreator[InKey]
) {
  val event: mutable.Map[InKey, (Value, Time)] = mutable.Map.empty
  val targetKeySet: Set[InKey] = fieldsKeysTimeoutsMs.keySet
  val keysIndexesMap: Map[InKey, Int] = targetKeySet.zip(0 until targetKeySet.size).toMap

  val extraFieldsIndexesMap: Map[InKey, Int] = extraFieldNames
    .zip(
      targetKeySet.size until
        targetKeySet.size + extraFieldNames.size
    )
    .toMap

  val allFieldsIndexesMap: Map[InKey, Int] = keysIndexesMap ++ extraFieldsIndexesMap
  val arity: Int = fieldsKeysTimeoutsMs.size + extraFieldNames.size

  var lastTimestamp = Time(Long.MinValue)
  var lastEvent: OutEvent = _
  val counter: AtomicLong = AtomicLong(1)
  // TODO: Timer

  val log = Logger[SparseRowsDataAccumulator[InEvent, InKey, Value, OutEvent]]

  log.debug(s"Created accumulator with fields map: ${allFieldsIndexesMap}")

  def map(item: InEvent): Seq[OutEvent] = {
    val time = extractTime(item)
    val delta = time.toMillis - lastTimestamp.toMillis
    var generatedEvents = mutable.ListBuffer[OutEvent]()
    if (regularityInterval.isDefined && lastEvent != null && 0 < delta && delta < eventsMaxGapMs) {
      // need to output multiple lines
      val linesCount = delta / regularityInterval.get
      val times = (1L to linesCount).map(i => lastTimestamp.toMillis + i * regularityInterval.get)
      times.foreach { t =>
        dropExpiredKeys(event, Time(t))
        val list = mutable.ListBuffer.tabulate[(InKey, AnyRef)](arity)(x => (keyCreator.create(s"empty_$x"), null))
        val indexesMap = if (defaultTimeout.isDefined) allFieldsIndexesMap else keysIndexesMap
        event.foreach {
          case (k, (v, _)) if indexesMap.contains(k) => list(indexesMap(k)) = (k, v.asInstanceOf[AnyRef])
          case _                                     => // do nothing
        }
        val e = eventCreator.create(list.toSeq, counter.get())
        counter.incrementAndGet()
        generatedEvents += e
      }
      log.info(s"Generated ${generatedEvents.length} events: $generatedEvents")
    }
    if (useUnfolding) {
      val (key, value) = extractKeyAndVal(item)
      if (event.get(key).orNull == null || value != null) event(key) = (value, time)
    } else {
      allFieldsIndexesMap.keySet.foreach { key =>
        val newValue = Try(extractValue(item, key))
        newValue match {
          case Success(nv) if nv != null || !event.contains(key) => event(key) = (nv.asInstanceOf[Value], time)
          case _                                                 => // do nothing
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
    extraFieldNames.foreach { name =>
      val value = extractValue(item, name)
      if (value != null) list(extraFieldsIndexesMap(name)) = (name, value.asInstanceOf[AnyRef])
    }
    val outEvent = eventCreator.create(list.toSeq, counter.get())
    val returnEvent = if (delta > 0 && lastEvent != null) {
      log.debug(s"Returning event: ${eventPrinter.prettyPrint(lastEvent)}")
      counter.incrementAndGet()
      generatedEvents += lastEvent
      generatedEvents.toSeq
    } else {
      Seq.empty
    }
    lastTimestamp = time
    lastEvent = outEvent
    returnEvent
  }

  def getLastEvent: OutEvent = lastEvent

  private def dropExpiredKeys(event: mutable.Map[InKey, (Value, Time)], currentRowTime: Time): Unit = {
    event.retain((k, v) =>
      currentRowTime.toMillis - v._2.toMillis < fieldsKeysTimeoutsMs.getOrElse(k, defaultTimeout.getOrElse(0L))
    )
  }

}

object SparseRowsDataAccumulator {

  def apply[InEvent, InKey, Value, OutEvent](
    streamSource: StreamSource[InEvent, InKey, Value],
    patternFields: Set[InKey]
  )(implicit
    timeExtractor: TimeExtractor[InEvent],
    extractKeyVal: InEvent => (InKey, Value),
    extractAny: Extractor[InEvent, InKey, Value],
    eventCreator: EventCreator[OutEvent, InKey],
    eventPrinter: EventPrinter[OutEvent],
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
            .filterNot { case (name, _) =>
              name == sparseRowsConf.keyColumn || name == sparseRowsConf.defaultValueColumn
            }
            .keys
            .toSeq
          new SparseRowsDataAccumulator(
            timeouts,
            extraFields.map(streamSource.fieldToEKey),
            useUnfolding = true,
            defaultTimeout = ndu.defaultTimeout,
            eventsMaxGapMs = streamSource.conf.eventsMaxGapMs.getOrElse(60000),
            regularityInterval = ndu.regularityInterval
          )(
            timeExtractor,
            extractKeyVal,
            extractAny,
            eventCreator,
            eventPrinter,
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
              .filterNot { case (name, _) =>
                sparseRowsConf.fieldsTimeoutsMs.contains(toKey(name))
              }
              .keys
              .toSeq
          new SparseRowsDataAccumulator(
            timeouts,
            extraFields.map(streamSource.fieldToEKey),
            useUnfolding = false,
            defaultTimeout = wdf.defaultTimeout,
            eventsMaxGapMs = streamSource.conf.eventsMaxGapMs.getOrElse(60000),
            regularityInterval = wdf.regularityInterval
          )(
            timeExtractor,
            extractKeyVal,
            extractAny,
            eventCreator,
            eventPrinter,
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
