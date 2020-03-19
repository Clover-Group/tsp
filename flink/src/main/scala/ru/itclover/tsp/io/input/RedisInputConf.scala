package ru.itclover.tsp.io.input

import ru.itclover.tsp.RowWithIdx

/**
  * Source for Redis Input
  * @param url connection string to redis
  * @param datetimeField name of datetime field, could be timestamp and regular time (will be parsed by JodaTime)
  * @param partitionFields fields by which data will be split and paralleled physically
  * @param fieldsTypes description of field and his type
  * @param key by what key get data from redis
  * @param serializer format of data in redis
  */
@SerialVersionUID(4815162342L)
@deprecated("Redis support will be dropped", "0.16.0")
case class RedisInputConf(
  url: String,
  datetimeField: Symbol,
  partitionFields: Seq[Symbol],
  dataTransformation: Option[SourceDataTransformation[RowWithIdx, Symbol, Any]] = None,
  fieldsTypes: Map[String, String],
  key: String,
  serializer: String = "json"
) extends InputConf[RowWithIdx, Symbol, Any] {

  def chunkSizeMs: Option[Long] = Some(10L)
  def defaultEventsGapMs: Long = 0L
  def defaultToleranceFraction: Option[Double] = Some(0.1)
  def eventsMaxGapMs: Long = 1L
  def numParallelSources: Option[Int] = Some(1)
  def parallelism: Option[Int] = Some(1)
  def patternsParallelism: Option[Int] = Some(1)
  def sourceId: Int = 1

}
