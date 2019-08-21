package ru.itclover.tsp.io.input

import java.util.{Properties, UUID}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaConsumerBase}
import org.apache.flink.api.common.serialization.{TypeInformationSerializationSchema, DeserializationSchema}
import org.apache.flink.types.Row

case class KafkaInputConf(
  brokers: String,
  topic: String,
  group: String = UUID.randomUUID().toString,
  // sourceId: Int,
  // jdbcUrl: String,
  // query: String,
  // driverName: String,
  // datetimeField: Symbol,
  // eventsMaxGapMs: Long,
  // defaultEventsGapMs: Long,
  // chunkSizeMs: Option[Long],
  partitionFields: Seq[Symbol]
  // userName: Option[String] = None,
  // password: Option[String] = None,
  // dataTransformation: Option[SourceDataTransformation[Row, Int, Any]] = None,
  // defaultToleranceFraction: Option[Double] = None,
  // parallelism: Option[Int] = None,
  // numParallelSources: Option[Int] = Some(1),
  // patternsParallelism: Option[Int] = Some(1),
  // timestampMultiplier: Option[Double] = Some(1000.0)
  // offsetReset: String = "largest"
) extends InputConf[Row, Int, Any] {

  def chunkSizeMs: Option[Long] = ???

  def dataTransformation
    : Option[ru.itclover.tsp.io.input.SourceDataTransformation[org.apache.flink.types.Row, Int, Any]] = ???
  def datetimeField: Symbol = ???
  def defaultEventsGapMs: Long = ???
  def defaultToleranceFraction: Option[Double] = ???
  def eventsMaxGapMs: Long = ???
  def numParallelSources: Option[Int] = ???
  def parallelism: Option[Int] = ???
  // def partitionFields: Seq[Symbol] = ???
  def patternsParallelism: Option[Int] = ???
  def sourceId: Int = ???

}

object KafkaSource {

  def getSource[Event: TypeInformationSerializationSchema](
    kafkaConfig: KafkaInputConf
  ): FlinkKafkaConsumerBase[Event] = {

    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", kafkaConfig.brokers)
    kafkaProps.setProperty("group.id", kafkaConfig.group)
    kafkaProps.setProperty("auto.commit.enable", "false")
    // kafkaProps.setProperty("auto.offset.reset", kafkaConfig.offsetReset)

    val deserializer: DeserializationSchema[Event] = implicitly[DeserializationSchema[Event]]

    new FlinkKafkaConsumer[Event](kafkaConfig.topic, deserializer, kafkaProps)
  }

}
