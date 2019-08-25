package ru.itclover.tsp.services

import java.util.Properties
import scala.util.Try
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.common.serialization.SimpleStringSchema
import ru.itclover.tsp.io.input.KafkaInputConf

object KafkaService {

  // Stub
  def fetchFieldsTypesInfo(conf: KafkaInputConf): Try[Seq[(Symbol, Class[_])]] = Try(
    Seq(
      ('and, classOf[Int]),
      ('ts, classOf[String]),
      ('SpeedEngine, classOf[Double]),
      ('Speed, classOf[Double]),
      ('loco_num, classOf[String]),
      ('Section, classOf[String]),
      ('upload_id, classOf[String])
    )
  )

  // Stub
  def consumer(conf: KafkaInputConf) = {
    val props = new Properties
    props.setProperty("bootstrap.servers", conf.brokers)
    props.setProperty("group.id", conf.group)
    // //props.setProperty("client.id", "client0")
    props.setProperty("auto.offset.reset", "earliest"); // Always read topic from start

    new FlinkKafkaConsumer(conf.topic, new SimpleStringSchema, props)
  }

}
