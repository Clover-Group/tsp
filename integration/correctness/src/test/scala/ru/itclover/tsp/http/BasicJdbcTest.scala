package ru.itclover.tsp.http

import java.util.concurrent.{SynchronousQueue, ThreadPoolExecutor, TimeUnit}
import akka.http.scaladsl.model.StatusCodes
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
import ru.itclover.tsp.io.output.{JDBCOutputConf, NewRowSchema}
import ru.itclover.tsp.utils.Files

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

class BasicJdbcTest extends FlatSpec with SqlMatchers with ScalatestRouteTest with HttpService with ForAllTestContainer {

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

  val port = 8148
  implicit override val container = new JDBCContainer(
    "yandex/clickhouse-server:latest",
    port -> 8123 :: 9087 -> 9000 :: Nil,
    "ru.yandex.clickhouse.ClickHouseDriver",
    s"jdbc:clickhouse://localhost:$port/default",
    waitStrategy = Some(Wait.forHttp("/"))
  )

  val inputConf = JDBCInputConf(
    sourceId = 123,
    jdbcUrl = container.jdbcUrl,
    query = """select *, speed as "speed(1)(2)" from Test.SM_basic_wide""", // speed(1)(2) fancy colnames test
    driverName = container.driverName,
    datetimeField = 'datetime,
    eventsMaxGapMs = Some(60000L),
    defaultEventsGapMs = Some(1000L),
    chunkSizeMs = Some(900000L),
    partitionFields = Seq('series_id, 'mechanism_id)
  )

  val typeCastingInputConf = inputConf.copy(query = "select * from Test.SM_typeCasting_wide limit 1000")

  val rowSchema = NewRowSchema('series_storage, 'from, 'to, ('app, 1), 'id, 'subunit)

  val outputConf = JDBCOutputConf(
    "Test.SM_basic_patterns",
    rowSchema,
    s"jdbc:clickhouse://localhost:$port/default",
    "ru.yandex.clickhouse.ClickHouseDriver"
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

    Files.readResource("/sql/test-db-schema.sql")
         .mkString
         .split(";")
         .foreach(container.executeUpdate)

    Files.readResource("/sql/wide/source-schema.sql")
         .mkString.split(";")
         .foreach(container.executeUpdate)

    Files.readResource("/sql/wide/source-inserts.sql")
         .mkString
         .split(";")
         .foreach(container.executeUpdate)

    Files.readResource("/sql/sink-schema.sql")
         .mkString
         .split(";")
         .foreach(container.executeUpdate)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    container.stop()
  }

  "Basic assertions and forwarded fields" should "work for wide dense table" in {

    Post("/streamJob/from-jdbc/to-jdbc/?run_async=0", FindPatternsRequest("1", inputConf, outputConf, basicAssertions)) ~>
    route ~> check {
      status shouldEqual StatusCodes.OK

      checkByQuery(
        List(List(2.0)),
        "SELECT to - from FROM Test.SM_basic_patterns WHERE id = 1 and " +
        "visitParamExtractString(context, 'mechanism_id') = '65001'"
      )

      checkByQuery(
        List(List(1.0)),
        "SELECT to - from FROM Test.SM_basic_patterns WHERE id = 2 and " +
        "visitParamExtractString(context, 'mechanism_id') = '65001'"
      )
      checkByQuery(
        List(List(1.0)),
        "SELECT to - from FROM Test.SM_basic_patterns WHERE id = 2 and " +
        "visitParamExtractString(context, 'mechanism_id') = '65002'"
      )

//      checkByQuery(
//        List(List(1.0)),
//        "SELECT to - from FROM Test.SM_basic_patterns WHERE id = 3 and " +
//        "visitParamExtractString(context, 'mechanism_id') = '65001' and visitParamExtractFloat(context, 'speed') = 20.0"
//      )
    }
  }

  "Types casting" should "work for wide dense table" in {
    Post(
      "/streamJob/from-jdbc/to-jdbc/?run_async=0",
      FindPatternsRequest("1", typeCastingInputConf, outputConf, typesCasting)
    ) ~>
    route ~> check {
      status shouldEqual StatusCodes.OK

      checkByQuery(
        List(List(0.0)),
        "SELECT to - from FROM Test.SM_basic_patterns WHERE id = 10 AND " +
        "visitParamExtractString(context, 'mechanism_id') = '65001'"
      )
      checkByQuery(
        List(List(2.0)),
        "SELECT to - from FROM Test.SM_basic_patterns WHERE id = 11 AND " +
        "visitParamExtractString(context, 'mechanism_id') = '65001'"
      )
    }
  }
}
