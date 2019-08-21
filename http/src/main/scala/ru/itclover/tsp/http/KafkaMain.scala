package ru.itclover.tsp.http.kafka

import java.util.Properties
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import ru.itclover.tsp.http.kafka.Serdes._

object KafkaMain extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  // generate a Watermark every second
  // env.getConfig.setAutoWatermarkInterval(1000)
  env.setParallelism(1)

  val properties = new Properties()
  properties.setProperty("bootstrap.servers", "37.228.115.243:9092")
  // only required for Kafka 0.8
  // properties.setProperty("zookeeper.connect", "localhost:2181")
  properties.setProperty("group.id", "group5")

  // val consumer = new FlinkKafkaConsumer[String]("batch_record_small_stream_writer", new SimpleStringSchema, properties)
  val consumer = new FlinkKafkaConsumer("batch_record_small_stream_writer", new StringDeserializer, properties)
  val stream = env.addSource(consumer).print

  // val out = for {
  //   cons <- new FlinkKafkaConsumer("batch_record_small_stream_writer", new ArrowDeserializer, properties)
  //   stream <- env.addSource(cons).print

  // } yield stream

  env.execute()

}
