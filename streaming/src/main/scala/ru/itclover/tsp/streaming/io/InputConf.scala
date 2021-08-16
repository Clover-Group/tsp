package ru.itclover.tsp.streaming.io

import java.util.UUID

trait InputConf[Event, EKey, EItem] extends Serializable {
  def sourceId: Int // todo .. Rm

  def datetimeField: Symbol
  def partitionFields: Seq[Symbol]
  def unitIdField: Option[Symbol] // Only for new sink, will be ignored for old

  def parallelism: Option[Int] // Parallelism per each source
  def numParallelSources: Option[Int] // Number on parallel (separate) sources to be created
  def patternsParallelism: Option[Int] // Number of parallel branches after source step

  def eventsMaxGapMs: Option[Long]
  def defaultEventsGapMs: Option[Long]
  def chunkSizeMs: Option[Long] // Chunk size

  def dataTransformation: Option[SourceDataTransformation[Event, EKey, EItem]]

  def defaultToleranceFraction: Option[Double]

  // Set maximum number of physically independent partitions for stream.keyBy operation
  def maxPartitionsParallelism: Int = 8192
}

case class JDBCInputConf[Event](
  sourceId: Int,
  jdbcUrl: String,
  query: String,
  driverName: String,
  datetimeField: Symbol,
  eventsMaxGapMs: Option[Long],
  defaultEventsGapMs: Option[Long],
  chunkSizeMs: Option[Long],
  partitionFields: Seq[Symbol],
  unitIdField: Option[Symbol] = None,
  userName: Option[String] = None,
  password: Option[String] = None,
  dataTransformation: Option[SourceDataTransformation[Event, Symbol, Any]] = None,
  defaultToleranceFraction: Option[Double] = None,
  parallelism: Option[Int] = None,
  numParallelSources: Option[Int] = Some(1),
  patternsParallelism: Option[Int] = Some(1),
  timestampMultiplier: Option[Double] = Some(1000.0)
) extends InputConf[Event, Symbol, Any]

case class KafkaInputConf[Event](
  sourceId: Int,
  brokers: String,
  topic: String,
  group: String = UUID.randomUUID().toString,
  startingOffsets: Option[String] = None,
  partitions: Option[List[Int]] = None,
  serializer: Option[String] = Some("json"),
  datetimeField: Symbol,
  eventsMaxGapMs: Option[Long],
  defaultEventsGapMs: Option[Long],
  chunkSizeMs: Option[Long],
  unitIdField: Option[Symbol] = None,
  partitionFields: Seq[Symbol],
  dataTransformation: Option[SourceDataTransformation[Event, Symbol, Any]] = None,
  timestampMultiplier: Option[Double] = Some(1000.0),
  fieldsTypes: Map[String, String]
) extends InputConf[Event, Symbol, Any] {
  def defaultToleranceFraction: Option[Double] = Some(0.1)
  def numParallelSources: Option[Int] = Some(1)
  def parallelism: Option[Int] = Some(1)
  def patternsParallelism: Option[Int] = Some(1)
}
