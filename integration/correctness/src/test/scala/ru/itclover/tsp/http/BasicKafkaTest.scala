package ru.itclover.tsp.http

import java.util.Properties
import java.util.concurrent.{SynchronousQueue, ThreadPoolExecutor, TimeUnit}
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import com.dimafeng.testcontainers._
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.scalatest.FlatSpec
import ru.itclover.tsp.core.RawPattern
import ru.itclover.tsp.http.domain.input.FindPatternsRequest
import ru.itclover.tsp.http.utils.SqlMatchers
import ru.itclover.tsp.io.input.KafkaInputConf
import ru.itclover.tsp.io.output.{KafkaOutputConf, NewRowSchema}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent._
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

// In test cases, 'should' expressions are non-unit. Suppressing wartremover warnings about it
@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class BasicKafkaTest
    extends FlatSpec
    with SqlMatchers
    with ScalatestRouteTest
    with HttpService
    with ForAllTestContainer {

  implicit override val executionContext: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global
  implicit override val streamEnvironment: StreamExecutionEnvironment =
    StreamExecutionEnvironment.createLocalEnvironment()
  streamEnvironment.setMaxParallelism(30000) // For proper keyBy partitioning

  val spark = SparkSession.builder()
    .master("local")
    .appName("TSP Spark test")
    .config("spark.io.compression.codec", "snappy")
    .getOrCreate()

  // to run blocking tasks.
  val blockingExecutorContext: ExecutionContextExecutor =
    ExecutionContext.fromExecutor(
      new ThreadPoolExecutor(
        0, // corePoolSize
        Int.MaxValue, // maxPoolSize
        1000L, //keepAliveTime
        TimeUnit.MILLISECONDS, //timeUnit
        new SynchronousQueue[Runnable](), //workQueue
        new ThreadFactoryBuilder().setNameFormat("blocking-thread").setDaemon(true).build()
      )
    )

  implicit def defaultTimeout = RouteTestTimeout(300.seconds)

  val container: KafkaContainer = KafkaContainer()
  def servers = container.kafkaContainer.getBootstrapServers.replaceAll("PLAINTEXT://", "")

  private val inputTopic = "input_topic"
  private val outputTopic = "output_topic"

  private val partitionFields = Seq('series_id, 'mechanism_id)
  val rowSchema = NewRowSchema('series_storage, 'from, 'to, ('app, 1), 'id, 'subunit)

  private def kafkaInputConf() = KafkaInputConf(
    brokers = servers,
    topic = inputTopic,
    datetimeField = 'timestamp,
    partitionFields = partitionFields,
    fieldsTypes = Map("timestamp" -> "int64", "series_id" -> "string", "mechanism_id" -> "string", "speed" -> "float64"),
    serializer = Some("json"),
    eventsMaxGapMs = Some(100000L)
  )

  private def kafkaOutputConf() = KafkaOutputConf(
    servers,
    outputTopic,
    Some("json"),
    rowSchema
  )

  val messages = Seq(
    """{"timestamp": 1410127755,"series_id": "series1","mechanism_id": "65001","speed": 0.0}""",
    """{"timestamp": 1410127756,"series_id": "series1","mechanism_id": "65001","speed": 5.0}""",
    """{"timestamp": 1410127757,"series_id": "series1","mechanism_id": "65001","speed": 10.0}""",
    """{"timestamp": 1410127758,"series_id": "series1","mechanism_id": "65001","speed": 15.0}""",
    """{"timestamp": 1410127759,"series_id": "series1","mechanism_id": "65001","speed": 20.0}""",
    """{"timestamp": 1410127761,"series_id": "series1","mechanism_id": "65002","speed": 20.0}""",
    """{"timestamp": 1410127762,"series_id": "series1","mechanism_id": "65002","speed": 20.0}"""
  )

  val basicAssertions = Seq(
    RawPattern(1, "speed < 15"),
    RawPattern(2, """"speed" > 10"""),
    RawPattern(3, "speed > 10.0", Some(Map("test" -> "test")), Some(540), Some(Seq('speed)))
  )
  val windowPattern = Seq(RawPattern(20, "speed < 20 for 1 sec"))

  lazy val producer: KafkaProducer[String, String] = {
    val props = new Properties()
    props.put("bootstrap.servers", servers)

    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    new KafkaProducer[String, String](props)
  }

  def sendToKafka(topic: String, key: String, value: String): Future[RecordMetadata] = {
    def producerCallback(promise: Promise[RecordMetadata]): Callback =
      (metadata: RecordMetadata, exception: Exception) => {
        val result = if (exception == null) Success(metadata) else Failure(exception)
        promise.complete(result)
      }

    val promise = Promise[RecordMetadata]()
    producer.send(new ProducerRecord(topic, key, value), producerCallback(promise))
    promise.future
  }

  lazy val kafkaConsumer: KafkaConsumer[String, String] = {
    val properties = new Properties()
    properties.put("bootstrap.servers", servers)
    properties.put("group.id", "consumer-tutorial")
    properties.put("key.deserializer", classOf[StringDeserializer])
    properties.put("value.deserializer", classOf[StringDeserializer])
    properties.put("auto.offset.reset", "earliest")

    new KafkaConsumer[String, String](properties)
  }

  def readFromKafkaForDuration(
    duration: scala.concurrent.duration.Duration,
    topics: String*
  ): Seq[ConsumerRecord[String, String]] = {
    import scala.collection.JavaConverters._
    kafkaConsumer.subscribe(topics.asJava)
    val currentTime = System.currentTimeMillis()
    var results = ArrayBuffer.empty[ConsumerRecord[String, String]]
    while (System.currentTimeMillis() - currentTime < duration.toMillis) {
      results ++= kafkaConsumer.poll(java.time.Duration.ofSeconds(1)).asScala
    }
    results
  }

  override def afterStart(): Unit = {
    super.afterStart()

    container.start()
    def sendToInputTopic(value: String) = sendToKafka(inputTopic, "0", value)

    val sentMessages = Future.sequence(messages.map(sendToInputTopic))

    Await.result(sentMessages, 10.seconds)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    container.stop()
  }
  "Basic assertions and forwarded fields" should "work for wide dense table" in {

    Post(
      "/streamJob/from-kafka/to-kafka/?run_async=1",
      FindPatternsRequest("1", kafkaInputConf(), kafkaOutputConf(), basicAssertions)
    ) ~>
    route ~> check {
      status shouldEqual StatusCodes.OK
    }

    val result = readFromKafkaForDuration(10.seconds, outputTopic)
    result.size shouldBe 11
  }

  "Simple window pattern" should "work for wide dense table" in {

    Post(
      "/streamJob/from-kafka/to-kafka/?run_async=1",
      FindPatternsRequest("2", kafkaInputConf(), kafkaOutputConf(), windowPattern)
    ) ~>
    route ~> check {
      status shouldEqual StatusCodes.OK
    }

    val result = readFromKafkaForDuration(20.seconds, outputTopic)
    result.size shouldBe 1
  }

}
