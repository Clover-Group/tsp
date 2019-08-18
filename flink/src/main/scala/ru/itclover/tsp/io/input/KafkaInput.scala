package ru.itclover.tsp.io.input

import java.util.{Properties, UUID}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaConsumerBase}
import org.apache.flink.api.common.serialization.{TypeInformationSerializationSchema, DeserializationSchema}
import org.apache.flink.types.Row

case class KafkaInputConf(
  brokers: String,
  topic: String,
  group: String = UUID.randomUUID().toString,
  offsetReset: String = "largest"
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
  def partitionFields: Seq[Symbol] = ???
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
    kafkaProps.setProperty("auto.offset.reset", kafkaConfig.offsetReset)

    val deserializer: DeserializationSchema[Event] = implicitly[DeserializationSchema[Event]]

    new FlinkKafkaConsumer010[Event](kafkaConfig.topic, deserializer, kafkaProps)
  }

}
