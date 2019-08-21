package ru.itclover.tsp.io.input

import java.util.{Properties, UUID}

import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaConsumerBase}
import org.apache.flink.types.Row
import ru.itclover.tsp.deserializers.KafkaArrowDeserializer

case class InputData(id: Int)

case class KafkaConf(
  brokers: String,
  topic: String,
  group: String = UUID.randomUUID().toString,
  offsetReset: String = "largest",
  sourceId: Int,
  datetimeField: Symbol,
  eventsMaxGapMs: Long,
  defaultEventsGapMs: Long,
  chunkSizeMs: Option[Long],
  partitionFields: Seq[Symbol],
  dataTransformation: Option[SourceDataTransformation[Row, Int, Any]] = None,
  defaultToleranceFraction: Option[Double] = None,
  parallelism: Option[Int] = None,
  numParallelSources: Option[Int] = Some(1),
  patternsParallelism: Option[Int] = Some(1),
)extends InputConf[Row, Int, Any]

object KafkaInputConf {

  def getSource(kafkaConfig: KafkaConf): FlinkKafkaConsumerBase[InputData] = {

    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", kafkaConfig.brokers)
    kafkaProps.setProperty("group.id", kafkaConfig.group)
    kafkaProps.setProperty("auto.commit.enable", "false")
    kafkaProps.setProperty("auto.offset.reset", kafkaConfig.offsetReset)

    new FlinkKafkaConsumer010[InputData](kafkaConfig.topic, KafkaArrowDeserializer, kafkaProps)
  }

}
