package ru.itclover.streammachine.io.input

import java.util.Properties

import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaConsumerBase}
import org.apache.flink.streaming.util.serialization.{DeserializationSchema, TypeInformationSerializationSchema}

object KafkaInput {

  def getSource[Event: TypeInformationSerializationSchema](kafkaConfig: KafkaConf): FlinkKafkaConsumerBase[Event] = {

    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", kafkaConfig.brokers)
    kafkaProps.setProperty("group.id", kafkaConfig.group)
    kafkaProps.setProperty("auto.commit.enable", "false")
    kafkaProps.setProperty("auto.offset.reset", kafkaConfig.offsetReset)

    val deserializer: DeserializationSchema[Event] = implicitly[DeserializationSchema[Event]]

    new FlinkKafkaConsumer010[Event](kafkaConfig.topic, deserializer, kafkaProps)
  }

}
