package ru.itclover.tsp.io.input

import org.apache.flink.types.Row

/**
* Information for deserialization protocols
  * @param key key from Redis
  * @param serializerType deserialization type
  */
case class DeserializationInfo(
  key: String,
  serializerType: String
)

/**
* Source for Redis Input
  * @param host host for Redis instance
  * @param port port for Redis instance
  * @param database number of Redis database(optional)
  * @param password password of Redis database(optional)
  * @param datetimeField name of datetime field, could be timestamp and regular time (will be parsed by JodaTime)
  * @param partitionFields fields by which data will be split and paralleled physically
  * @param fieldsTypes description of field and his type
  * @param serializationInfo list of DeserializationInfo objects
  */
@SerialVersionUID(4815162342L)
case class RedisInputConf(
  host: String,
  port: Int,
  database: Option[Int] = None,
  password: Option[String] = None,
  datetimeField: Symbol,
  partitionFields: Seq[Symbol],
  dataTransformation: Option[SourceDataTransformation[Row, Symbol, Any]] = None,
  fieldsTypes: Map[String, String],
  serializationInfo: Seq[DeserializationInfo]
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
