package ru.itclover.tsp.transformers

import com.typesafe.scalalogging.Logger
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import ru.itclover.tsp.phases.NumericPhases.SymbolNumberExtractor
import ru.itclover.tsp.EvalUtils
import ru.itclover.tsp.core.{Pattern, Time}
import ru.itclover.tsp.core.Time.{TimeExtractor, TimeNonTransformedExtractor}
import ru.itclover.tsp.io.{EventCreator, EventCreatorInstances}
import ru.itclover.tsp.io.input.{InputConf, JDBCInputConf, NarrowDataUnfolding, WideDataFilling}
import ru.itclover.tsp.phases.Phases.{AnyExtractor, AnyNonTransformedExtractor}

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
  implicit extractTime: TimeNonTransformedExtractor[InEvent],
  extractKeyAndVal: InEvent => (Symbol, Value),
  extractAny: AnyNonTransformedExtractor[InEvent],
  eventCreator: EventCreator[OutEvent]
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
        val newValue = Try(extractAny(item, key))
        newValue match {
          case Success(nv) if nv != null || !event.contains(key) => event(key) = (nv.asInstanceOf[Value], time)
        }
      }
    }
    dropExpiredKeys(event, time)
    if (!useUnfolding || (targetKeySet subsetOf event.keySet)) {
      val list = mutable.ListBuffer.tabulate[(Symbol, AnyRef)](arity)(x => (Symbol(s"empty_$x"), null))
      val indexesMap = if (defaultTimeout.isDefined) allFieldsIndexesMap else keysIndexesMap
      event.foreach {
        case (k, (v, _)) if indexesMap.contains(k) => list(indexesMap(k)) = (k, v.asInstanceOf[AnyRef])
        case _                                     =>
      }
      if (defaultTimeout.isEmpty) {
        extraFieldNames.foreach { name =>
          val value = extractAny(item, name)
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

  def apply[InEvent, Value, OutEvent](inputConf: InputConf[InEvent])(
    implicit timeExtractor: TimeNonTransformedExtractor[InEvent],
    extractKeyVal: InEvent => (Symbol, Value),
    extractAny: AnyNonTransformedExtractor[InEvent],
    rowTypeInfo: TypeInformation[OutEvent],
    eventCreator: EventCreator[OutEvent]
  ): SparseRowsDataAccumulator[InEvent, Value, OutEvent] = {
    inputConf.dataTransformation
      .map({
        case ndu: NarrowDataUnfolding =>
          val sparseRowsConf = ndu
          val fim = inputConf.errOrFieldsIdxMap match {
            case Right(m) => m
            case Left(e)  => sys.error(e.toString)
          }
          val extraFields = fim
            .filterNot(
              nameAndInd => nameAndInd._1 == sparseRowsConf.key || nameAndInd._1 == sparseRowsConf.value
            )
            .keys
            .toSeq
          SparseRowsDataAccumulator(
            sparseRowsConf.fieldsTimeoutsMs,
            extraFields,
            useUnfolding = true,
            defaultTimeout = ndu.defaultTimeout
          )(
            timeExtractor,
            extractKeyVal,
            extractAny,
            eventCreator
          )
        case wdf: WideDataFilling =>
          val sparseRowsConf = wdf
          val fim = inputConf.errOrFieldsIdxMap match {
            case Right(m) => m
            case Left(e)  => sys.error(e.toString)
          }
          val extraFields =
            fim.filterNot(nameAndInd => sparseRowsConf.fieldsTimeoutsMs.contains(nameAndInd._1)).keys.toSeq
          SparseRowsDataAccumulator(
            sparseRowsConf.fieldsTimeoutsMs,
            extraFields,
            useUnfolding = false,
            defaultTimeout = wdf.defaultTimeout
          )(
            timeExtractor,
            extractKeyVal,
            extractAny,
            eventCreator
          )
        case _ =>
          sys.error(
            s"Invalid config type: expected NarrowDataUnfolding or WideDataFilling, got ${inputConf.dataTransformation} instead"
          )
      })
      .getOrElse(sys.error("No data transformation config specified"))
  }

  def fieldsIndexesMap[InEvent](inputConf: InputConf[InEvent]): Map[Symbol, Int] = {
    val fim = inputConf.errOrFieldsIdxMap match {
      case Right(m) => m
      case Left(e)  => sys.error(e.toString)
    }
    val (extraFields, targetKeySet, useUnfolding) = inputConf.dataTransformation
      .map({
        case ndu: NarrowDataUnfolding =>
          (
            fim
              .filterNot(
                nameAndInd => nameAndInd._1 == ndu.key || nameAndInd._1 == ndu.value
              )
              .keys
              .toSeq,
            ndu.fieldsTimeoutsMs.keySet,
            true
          )
        case wdf: WideDataFilling =>
          (
            fim
              .filterNot(
                nameAndInd => wdf.fieldsTimeoutsMs.contains(nameAndInd._1)
              )
              .keys
              .toSeq,
            wdf.fieldsTimeoutsMs.keySet,
            false
          )
      })
      .getOrElse(sys.error("Invalid config type"))
    val keysIndexesMap: Map[Symbol, Int] = targetKeySet.zip(0 until targetKeySet.size).toMap

    val extraFieldsIndexesMap: Map[Symbol, Int] = extraFields
      .zip(
        targetKeySet.size until
        targetKeySet.size + extraFields.size
      )
      .toMap
    // TODO: Replace that ugly workaround
    val fieldsIndexesMap: Map[Symbol, Int] = keysIndexesMap ++ extraFieldsIndexesMap
    fieldsIndexesMap
  }

  def emptyEvent[InEvent](inputConf: InputConf[InEvent])(implicit eventCreator: EventCreator[InEvent]): InEvent = {
    eventCreator.emptyEvent(fieldsIndexesMap(inputConf))
  }
}
