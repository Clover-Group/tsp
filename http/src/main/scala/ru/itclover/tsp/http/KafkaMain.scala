package ru.itclover.tsp.http.kafka

import java.util.Properties
import org.apache.flink.streaming.api
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import ru.itclover.tsp.http.kafka.Serdes._
import org.apache.arrow.vector.VectorUnloader
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.arrow.vector.ipc.ArrowStreamReader

import org.apache.arrow.vector.types.Types.MinorType.{INT, BIGINT, VARCHAR, FLOAT8}
import ArrowPkg.ArrowOps._

object KafkaMain extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setRestartStrategy(RestartStrategies.noRestart)
  // env.enableCheckpointing(5000)
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

  val stream: DataStream[ArrowStreamReader] = env.addSource(consumer)

  val out: DataStream[Unit] = stream.map(str => {

    while (str.loadNextBatch) {

      val root = str.getVectorSchemaRoot
      val schema = root.getSchema
      val vectors = root.getFieldVectors

      println(s"Total vectors = ${vectors.size}")

      vectors.forEach(
        v =>
          v.getMinorType match {
            case BIGINT  => BigIntOps.unpack(v)
            case VARCHAR => VCharOps.unpack(v)
            case FLOAT8  => Float8Ops.unpack(v)
            case unknown => println(s"Unknown vector type: $unknown")
          }
      )
    }
  })

  env.execute()
}
