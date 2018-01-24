package ru.itclover.streammachine.transformers

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import ru.itclover.streammachine.{Eval, EvalUtils}
import ru.itclover.streammachine.core.{PhaseParser, Time}
import ru.itclover.streammachine.core.Time.TimeExtractor

import scala.collection.mutable
import scala.reflect.ClassTag


// Row -> subset(Row) + Row.keys-values-set
// Dt Key + Partition Keys + Rule Keys

/**
  * @param fieldsKeysTimeoutsMs - indexes to collect and timeouts (milliseconds) per each (collect by-hand for now)
  * @param keyValIndexes - row indexes for k/v
  * @param extraFieldIndexesAndNames - will be added to every emitting event
  */
case class SparseRowsDataAccumulator(fieldsKeysTimeoutsMs: Map[Symbol, Long],
                                     keyValIndexes: (Int, Int),
                                     extraFieldIndexesAndNames: Seq[(Int, Symbol)])
                                    (implicit extractTime: TimeExtractor[Row])
  extends RichFlatMapFunction[Row, Row] with Serializable {
  // potential event values with receive time
  val event: mutable.Map[Symbol, (Double, Time)] = mutable.Map.empty
  val targetKeySet: Set[Symbol] = fieldsKeysTimeoutsMs.keySet
  val fieldsIndexesMap: Map[Symbol, Int] = targetKeySet.zip(0 until targetKeySet.size).toMap
  val arity: Int = fieldsKeysTimeoutsMs.size + extraFieldIndexesAndNames.size

  override def flatMap(item: Row, out: Collector[Row]): Unit = {
    // Option[num_val]
    val key = Symbol(item.getField(keyValIndexes._1).toString) // TODO check
    val value = item.getField(keyValIndexes._2).asInstanceOf[Double]  // TODO assert

    val time = extractTime(item)
    event(key) = (value, time)
    dropExpiredKeys(event, time)
    if (targetKeySet subsetOf event.keySet) {
      val row = new Row(arity)
      // row.setField()
      event.foreach { case (k, (v, _)) => row.setField(fieldsIndexesMap(k), v) }
      extraFieldIndexesAndNames.foreach { case (ind, name) => row.setField(fieldsIndexesMap(name), item.getField(ind)) }
      out.collect(row)
    }
  }

  private def dropExpiredKeys(event: mutable.Map[Symbol, (Double, Time)], currentRowTime: Time): Unit = {
    event.retain((k, v) => currentRowTime.toMillis - v._2.toMillis < fieldsKeysTimeoutsMs(k))
  }
}

