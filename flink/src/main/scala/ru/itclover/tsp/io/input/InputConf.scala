package ru.itclover.tsp.io.input


trait InputConf[Event] extends Serializable {
  def sourceId: Int // todo .. Rm

  def datetimeField: Symbol
  def partitionFields: Seq[Symbol]

  def parallelism: Option[Int]          // Parallelism per each source
  def numParallelSources: Option[Int]   // Number on parallel (separate) sources to be created
  def patternsParallelism: Option[Int]  // Number of parallel branches after source step

  def eventsMaxGapMs: Long
  def defaultEventsGapMs: Long

  def dataTransformation: Option[SourceDataTransformation]
}
