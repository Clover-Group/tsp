package ru.itclover.tsp.streaming.io

trait InputConf[Event, EKey, EItem] extends Serializable {
  def datetimeField: Symbol
  def partitionFields: Seq[Symbol]
  def unitIdField: Option[Symbol] // Only for new sink, will be ignored for old

  def parallelism: Option[Int] // Parallelism per each source
  def numParallelSources: Option[Int] // Number on parallel (separate) sources to be created
  def patternsParallelism: Option[Int] // Number of parallel branches after source step

  def eventsMaxGapMs: Option[Long]
  def defaultEventsGapMs: Option[Long]
  def chunkSizeMs: Option[Long] // Chunk size
  def processingBatchSize: Option[Int]

  def dataTransformation: Option[SourceDataTransformation[Event, EKey, EItem]]

  // Set maximum number of physically independent partitions for stream.keyBy operation
  def maxPartitionsParallelism: Int = 8192
}