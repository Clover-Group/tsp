package ru.itclover.spark

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode}

case class Appevent(eventType: String, created: Long, userId: String)

object SparkStreamingJob extends App {

  val conf = new SparkConf().setAppName("StreamingMachine-Spark").setMaster("local[7]")

  val spark = SparkSession.builder().config(conf).getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val bootstrapServers = "10.30.5.229:9092,10.30.5.251:9092"

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

}