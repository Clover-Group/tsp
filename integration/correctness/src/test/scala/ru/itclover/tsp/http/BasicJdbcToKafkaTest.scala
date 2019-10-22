package ru.itclover.tsp.http

import java.util.concurrent.{SynchronousQueue, ThreadPoolExecutor, TimeUnit}

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import com.dimafeng.testcontainers._
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.scalatest.FlatSpec
import ru.itclover.tsp.core.RawPattern
import ru.itclover.tsp.http.domain.input.FindPatternsRequest
import ru.itclover.tsp.http.utils.{JDBCContainer, SqlMatchers}
import ru.itclover.tsp.io.input.JDBCInputConf
import ru.itclover.tsp.io.output.{KafkaOutputConf, RowSchema}
import ru.itclover.tsp.utils.Files

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

class BasicJdbcToKafkaTest extends FlatSpec with SqlMatchers with ScalatestRouteTest with HttpService with ForAllTestContainer {

  implicit override val executionContext: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global
  implicit override val streamEnvironment: StreamExecutionEnvironment =
    StreamExecutionEnvironment.createLocalEnvironment()
  streamEnvironment.setMaxParallelism(30000) // For proper keyBy partitioning

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

  implicit def defaultTimeout(implicit system: ActorSystem) = RouteTestTimeout(300.seconds)

  val port = 8170
  val clickhouseContainer = new JDBCContainer(
    "yandex/clickhouse-server:latest",
    port -> 8123 :: 9080 -> 9000 :: Nil,
    "ru.yandex.clickhouse.ClickHouseDriver",
    s"jdbc:clickhouse://localhost:$port/default"
  )

  val inputConf = JDBCInputConf(
    sourceId = 123,
    jdbcUrl = clickhouseContainer.jdbcUrl,
    query = """select *, speed as "speed(1)(2)" from Test.SM_basic_wide""", // speed(1)(2) fancy colnames test
    driverName = clickhouseContainer.driverName,
    datetimeField = 'datetime,
    eventsMaxGapMs = 60000L,
    defaultEventsGapMs = 1000L,
    chunkSizeMs = Some(900000L),
    partitionFields = Seq('series_id, 'mechanism_id)
  )

    val kafkaPort = 8092

    val kafkaContainer = KafkaContainer()

    implicit override val container = MultipleContainers(clickhouseContainer, kafkaContainer)

  val typeCastingInputConf = inputConf.copy(query = "select * from Test.SM_typeCasting_wide limit 1000")

  val rowSchema = RowSchema('series_storage, 'from, 'to, ('app, 1), 'id, 'timestamp, 'context, inputConf.partitionFields)

  val outputConf = KafkaOutputConf(
    "127.0.0.1:9092",
    "SM_basic_patterns",
    rowSchema
  )

  val basicAssertions = Seq(
    RawPattern("1", "speed < 15"),
    RawPattern("2", """"speed(1)(2)" > 10"""),
    RawPattern("3", "speed > 10.0", Map("test" -> "test"), Seq('speed))
  )
  val typesCasting = Seq(RawPattern("10", "speed = 15"), RawPattern("11", "speed64 < 15.0"))
  val errors = Seq(RawPattern("20", "speed = QWE 15"), RawPattern("21", "speed64 < 15.0"))

  override def afterStart(): Unit = {
    super.afterStart()
    Files.readResource("/sql/test-db-schema.sql").mkString.split(";").map(clickhouseContainer.executeUpdate)
    Files.readResource("/sql/wide/source-schema.sql").mkString.split(";").map(clickhouseContainer.executeUpdate)
    Files.readResource("/sql/wide/source-inserts.sql").mkString.split(";").map(clickhouseContainer.executeUpdate)
    Files.readResource("/sql/sink-schema.sql").mkString.split(";").map(clickhouseContainer.executeUpdate)
  }

  "Basic assertions and forwarded fields" should "work for wide dense table" in {

    Post("/streamJob/from-jdbc/to-kafka/?run_async=0", FindPatternsRequest("1", inputConf, outputConf, basicAssertions)) ~>
    route ~> check {
      entityAs[String] shouldBe ""
      status shouldEqual StatusCodes.OK
    }
  }

  "Types casting" should "work for wide dense table" in {
    Post(
      "/streamJob/from-jdbc/to-kafka/?run_async=0",
      FindPatternsRequest("1", typeCastingInputConf, outputConf, typesCasting)
    ) ~>
    route ~> check {
      entityAs[String] shouldBe ""
      status shouldEqual StatusCodes.OK

    }
  }
}
