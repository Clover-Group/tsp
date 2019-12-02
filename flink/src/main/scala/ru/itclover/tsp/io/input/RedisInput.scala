package ru.itclover.tsp.io.input

import org.apache.flink.types.Row

/**
  * Source for Redis Input
  * @param url connection string to redis
  * @param datetimeField name of datetime field, could be timestamp and regular time (will be parsed by JodaTime)
  * @param partitionFields fields by which data will be split and paralleled physically
  * @param fieldsTypes description of field and his type
  * @param key by what key get data from redis
  */
@SerialVersionUID(4815162342L)
case class RedisInputConf(
  url: String,
  datetimeField: Symbol,
  partitionFields: Seq[Symbol],
  dataTransformation: Option[SourceDataTransformation[Row, Symbol, Any]] = None,
  fieldsTypes: Map[String, String],
  key: String,
  serializer: String
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
