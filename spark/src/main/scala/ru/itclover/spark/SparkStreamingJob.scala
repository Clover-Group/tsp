package ru.itclover.akka

import java.util
import java.util.Properties

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.hadoop.yarn.state.StateMachine
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import ru.itclover.spark.{Phases, SparkMachineMapper}
import ru.itclover.streammachine.core
import ru.itclover.streammachine.core.PhaseParser.Functions.avg
import ru.itclover.streammachine.core.Window
import ru.itclover.streammachine.phases.NoState

import scala.reflect.ClassTag

case class Appevent(eventType: String, created: Long, userId: String)

object SparkStreamingJob extends App {

  val conf = new SparkConf().setAppName("StreamingMachine-Spark").setMaster("local[7]")

  val sc = new SparkContext(conf)

  val ssc = new StreamingContext(sc, Seconds(10))

  val spark = SparkSession.builder().config(conf).getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val bootstrapServers = "10.30.5.229:9092,10.30.5.251:9092"

  def getBasicStringStringConsumer(group: String = "MyGroup"): Properties = {
    val consumerConfig: Properties = new Properties
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, group)
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getCanonicalName)
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getCanonicalName)
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

    //consumerConfig.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY, "roundrobin")

    consumerConfig
  }

  val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
    ssc,
    LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String, String](
      java.util.Arrays.asList("common"),
      getBasicStringStringConsumer("spark-streaming-group").asInstanceOf[java.util.Map[String, Object]]
    )
  )

  import spark.implicits._

  val lines = spark
    .readStream
    .format("kafka")
    .option("kafka." + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    .option("subscribe", "common")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 50000)
    .load()

  val objectMapper = new ObjectMapper() with ScalaObjectMapper
  objectMapper.registerModule(DefaultScalaModule)
  objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  val javaType = objectMapper.constructType[Appevent]


  val appevents = lines
    .selectExpr("CAST(value AS STRING)")
    .as[String]
    .map(value => objectMapper.readValue[Appevent](value, javaType))


  //  appevents.groupByKey(_.userId).
  //  appevents.printSchema()

  //  val res = appevents.writeStream.outputMode("append").format("console").start()
  //  val res = appevents
  //    .writeStream
  //    .format("json")
  //    .option("path", "/tmp/spark/")
  //    .option("checkpointLocation", "/tmp/checkpoints2")
  //    .start()
  //
  //  res.awaitTermination()


  // Split the lines into words
  //  val words = lines.selectExpr("CAST(value AS STRING)").as[String].flatMap(_.split(" "))

  // Generate running word count

  import Implicits._

  //  appevents.withWatermark().groupByKey(_.userId).flatMapGroupsWithState(OutputMode.Append(),GroupStateTimeout.EventTimeTimeout()){
  val results = appevents.groupByKey(_.userId)
    .flatMapGroupsWithState(OutputMode.Append(), GroupStateTimeout.NoTimeout())(
      new SparkMachineMapper(Phases.phaseParser)
    )


  val query = results.writeStream
    //    .outputMode("complete")
    .format("console")
    .start()

  query.awaitTermination()


}

object Implicits {
  //  implicit val unitEncoder: Encoder[NoState] = org.apache.spark.sql.Encoders.javaSerialization[NoState]
  //  implicit val windowEncoder: Encoder[Window] = org.apache.spark.sql.Encoders.javaSerialization(ClassTag.apply[Window](classOf[ru.itclover.streammachine.core.Window]))

}