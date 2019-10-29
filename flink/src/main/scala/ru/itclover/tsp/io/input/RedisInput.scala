package ru.itclover.tsp.io.input

import org.apache.flink.types.Row

case class RedisInputConf(
  host: String,
  port: Int,
  database: Option[String] = None,
  password: Option[String] = None,
  clusterNodes: Option[Seq[String]] = None,
  datetimeField: Symbol,
  partitionFields: Seq[Symbol],
  dataTransformation: Option[SourceDataTransformation[Row, Symbol, Any]] = None,
  fieldsTypes: Map[String, String]
) extends InputConf[Row, Symbol, Any] {

  def chunkSizeMs: Option[Long] = Some(10L)
  def defaultEventsGapMs: Long = 0L
  def defaultToleranceFraction: Option[Double] = Some(0.1)
  def eventsMaxGapMs: Long = 1L
  def numParallelSources: Option[Int] = Some(1)
  def parallelism: Option[Int] = Some(1)
  def patternsParallelism: Option[Int] = Some(1)
  def sourceId: Int = 1

}
