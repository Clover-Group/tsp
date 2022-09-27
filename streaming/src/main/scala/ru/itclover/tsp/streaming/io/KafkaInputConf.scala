package ru.itclover.tsp.streaming.io

import java.util.UUID
import ru.itclover.tsp.RowWithIdx

case class KafkaInputConf(
                           sourceId: Int,
                           brokers: String,
                           topic: String,
                           group: String = UUID.randomUUID().toString,
                           serializer: Option[String] = Some("json"),
                           datetimeField: Symbol,
                           partitionFields: Seq[Symbol],
                           unitIdField: Option[Symbol] = None,
                           dataTransformation: Option[SourceDataTransformation[RowWithIdx, Symbol, Any]] = None,
                           timestampMultiplier: Option[Double] = Some(1000.0),
                           eventsMaxGapMs: Option[Long] = Some(90000),
                           chunkSizeMs: Option[Long] = Some(10L),
                           processingBatchSize: Option[Int],
                           numParallelSources: Option[Int] = Some(1),
                           fieldsTypes: Map[String, String]
                         ) extends InputConf[RowWithIdx, Symbol, Any] {

  def defaultEventsGapMs: Option[Long] = Some(0L)
  def defaultToleranceFraction: Option[Double] = None
  def parallelism: Option[Int] = Some(1)
  def patternsParallelism: Option[Int] = Some(1)
}
