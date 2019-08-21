package ru.itclover.tsp.http.kafka

import java.util.Properties
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import ru.itclover.tsp.http.kafka.Serdes._

object KafkaMain extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  // generate a Watermark every second
  env.getConfig.setAutoWatermarkInterval(1000)
  env.setParallelism(1)

  val props = new Properties
  props.setProperty("bootstrap.servers", "37.228.115.243:9092")
  props.setProperty("group.id", "group1")
  //props.setProperty("client.id", "client0")
  props.setProperty("auto.offset.reset", "earliest"); // Always read topic from start

  // val topic = "batch_record_small_stream_writer"
  val topic = "batch_record_100_285"
  val consumer = new FlinkKafkaConsumer(topic, new ArrowDeserializer, props)

  val stream = env.addSource(consumer).map(r => println(r.getVectorSchemaRoot.getSchema))

  env.execute()
}
