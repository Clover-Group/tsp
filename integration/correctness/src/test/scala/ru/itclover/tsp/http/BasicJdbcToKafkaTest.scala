package ru.itclover.tsp.http

import java.util.concurrent.{SynchronousQueue, ThreadPoolExecutor, TimeUnit}
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import com.dimafeng.testcontainers._
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.spark.sql.SparkSession
import org.scalatest.FlatSpec
import org.testcontainers.containers.wait.strategy.Wait
import ru.itclover.tsp.core.RawPattern
import ru.itclover.tsp.http.domain.input.FindPatternsRequest
import ru.itclover.tsp.http.utils.{JDBCContainer, SqlMatchers}
import ru.itclover.tsp.io.input.JDBCInputConf
import ru.itclover.tsp.io.output.{KafkaOutputConf, NewRowSchema}
import ru.itclover.tsp.utils.Files

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class BasicJdbcToKafkaTest
    extends FlatSpec
    with SqlMatchers
    with ScalatestRouteTest
    with HttpService
    with ForAllTestContainer {

  implicit override val executionContext: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global
  implicit override val streamEnvironment: StreamExecutionEnvironment =
    StreamExecutionEnvironment.createLocalEnvironment()
  streamEnvironment.setParallelism(4) // To prevent run out of network buffers on large number of CPUs (e.g. 32)
  streamEnvironment.setMaxParallelism(30000) // For proper keyBy partitioning

  val spark = SparkSession
    .builder()
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

  val port = 8170

  val clickhouseContainer = new JDBCContainer(
    "yandex/clickhouse-server:latest",
    port -> 8123 :: 9080 -> 9000 :: Nil,
    "ru.yandex.clickhouse.ClickHouseDriver",
    s"jdbc:clickhouse://localhost:$port/default",
    waitStrategy = Some(Wait.forHttp("/").forStatusCode(200).forStatusCode(400))
  )

  val inputConf = JDBCInputConf(
    sourceId = 123,
    jdbcUrl = clickhouseContainer.jdbcUrl,
    query = """select *, speed as "speed(1)(2)" from Test.SM_basic_wide""", // speed(1)(2) fancy colnames test
    driverName = clickhouseContainer.driverName,
    datetimeField = 'datetime,
    unitIdField = Some('unit),
    eventsMaxGapMs = Some(60000L),
    defaultEventsGapMs = Some(1000L),
    chunkSizeMs = Some(900000L),
    partitionFields = Seq('series_id, 'mechanism_id)
  )

  val kafkaPort = 8092

  val kafkaContainer = KafkaContainer()

  implicit override val container = MultipleContainers(clickhouseContainer, kafkaContainer)

  val typeCastingInputConf = inputConf.copy(query = "select * from Test.SM_typeCasting_wide limit 1000")

  val rowSchema = NewRowSchema('series_storage, 'from, 'to, ('app, 1), 'id, 'subunit)

  val outputConf = KafkaOutputConf(
    "127.0.0.1:9092",
    "SM_basic_patterns",
    Some("json"),
    rowSchema
  )

  val basicAssertions = Seq(
    RawPattern(1, "speed < 15"),
    RawPattern(2, """"speed(1)(2)" > 10"""),
    RawPattern(3, "speed > 10.0", Some(Map("test" -> "test")), Some(540), Some(Seq('speed)))
  )
  val typesCasting = Seq(RawPattern(10, "speed = 15"), RawPattern(11, "speed64 < 15.0"))
  val errors = Seq(RawPattern(20, "speed = QWE 15"), RawPattern(21, "speed64 < 15.0"))

  override def afterStart(): Unit = {
    super.afterStart()

    Files
      .readResource("/sql/test-db-schema.sql")
      .mkString
      .split(";")
      .foreach(clickhouseContainer.executeUpdate)

    Files
      .readResource("/sql/wide/source-schema.sql")
      .mkString
      .split(";")
      .foreach(clickhouseContainer.executeUpdate)

    Files
      .readResource("/sql/wide/source-inserts.sql")
      .mkString
      .split(";")
      .foreach(clickhouseContainer.executeUpdate)

    Files
      .readResource("/sql/sink-schema.sql")
      .mkString
      .split(";")
      .foreach(clickhouseContainer.executeUpdate)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    container.stop()
  }

  "Basic assertions and forwarded fields" should "work for wide dense table" in {

    Post("/streamJob/from-jdbc/to-kafka/?run_async=0", FindPatternsRequest("1", inputConf, outputConf, basicAssertions)) ~>
    route ~> check {
      //status shouldEqual StatusCodes.OK
    }
  }

  "Types casting" should "work for wide dense table" in {
    Post(
      "/streamJob/from-jdbc/to-kafka/?run_async=0",
      FindPatternsRequest("1", typeCastingInputConf, outputConf, typesCasting)
    ) ~>
    route ~> check {
      //status shouldEqual StatusCodes.OK
    }
  }
}
