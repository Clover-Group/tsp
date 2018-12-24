package ru.itclover.tsp.io.input


trait InputConf[Event, EKey, EItem] extends Serializable {
  def sourceId: Int // todo .. Rm

  def datetimeField: Symbol
  def partitionFields: Seq[Symbol]

  def parallelism: Option[Int]          // Parallelism per each source
  def numParallelSources: Option[Int]   // Number on parallel (separate) sources to be created
  def patternsParallelism: Option[Int]  // Number of parallel branches after source step

  def eventsMaxGapMs: Long
  def defaultEventsGapMs: Long

  def dataTransformation: Option[SourceDataTransformation[Event, EKey, EItem]]

  def defaultToleranceFraction: Option[Double]

  // Set maximum number of physically independent partitions for stream.keyBy operation
  def maxPartitionsParallelism: Int = 8192
}
