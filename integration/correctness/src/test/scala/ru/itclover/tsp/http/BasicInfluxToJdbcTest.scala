package ru.itclover.tsp.http

import java.util.concurrent.{SynchronousQueue, ThreadPoolExecutor, TimeUnit}
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import com.dimafeng.testcontainers._
//import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.scalatest.FlatSpec
import org.testcontainers.containers.wait.strategy.Wait
import ru.itclover.tsp.core.RawPattern
import ru.itclover.tsp.http.domain.input.FindPatternsRequest
import ru.itclover.tsp.http.utils.{InfluxDBContainer, JDBCContainer, SqlMatchers}
import ru.itclover.tsp.io.input.{InfluxDBInputConf, WideDataFilling}
import ru.itclover.tsp.io.output.{JDBCOutputConf, NewRowSchema}
import ru.itclover.tsp.utils.Files

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

// In test cases, 'should' expressions are non-unit. Suppressing wartremover warnings about it
// Also, IO configurations use Any.
@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements", "org.wartremover.warts.Any"))
class BasicInfluxToJdbcTest
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

  // to run blocking tasks.
  val blockingExecutorContext: ExecutionContextExecutor =
    ExecutionContext.fromExecutor(
      new ThreadPoolExecutor(
        0, // corePoolSize
        Int.MaxValue, // maxPoolSize
        1000L, //keepAliveTime
        TimeUnit.MILLISECONDS, //timeUnit
        new SynchronousQueue[Runnable](), //workQueue
        //new ThreadFactoryBuilder().setNameFormat("blocking-thread").setDaemon(true).build()
      )
    )

  implicit def defaultTimeout = RouteTestTimeout(300.seconds)

  val influxPort = 8138

  val influxContainer =
    new InfluxDBContainer(
      "influxdb:1.5",
      influxPort -> 8086 :: Nil,
      s"http://localhost:$influxPort",
      "Test",
      "default",
      waitStrategy = Some(Wait.forHttp("/").forStatusCode(200).forStatusCode(404))
    )

  val jdbcPort = 8157
  implicit val jdbcContainer = new JDBCContainer(
    "yandex/clickhouse-server:latest",
    jdbcPort -> 8123 :: 9072 -> 9000 :: Nil,
    "ru.yandex.clickhouse.ClickHouseDriver",
    s"jdbc:clickhouse://localhost:$jdbcPort/default",
    waitStrategy = Some(Wait.forHttp("/").forStatusCode(200).forStatusCode(400))
  )

  override val container = MultipleContainers(LazyContainer(jdbcContainer), LazyContainer(influxContainer))

  val inputConf = InfluxDBInputConf(
    sourceId = 123,
    url = influxContainer.url,
    query = "select * from SM_basic_wide",
    dbName = influxContainer.dbName,
    eventsMaxGapMs = Some(60000L),
    defaultEventsGapMs = Some(1000L),
    chunkSizeMs = Some(900000L),
    unitIdField = Some('mechanism_id),
    partitionFields = Seq('series_id, 'mechanism_id),
    userName = Some("default"),
    additionalTypeChecking = Some(false)
  )
  val typeCastingInputConf = inputConf.copy(query = """select * from SM_typeCasting_wide""")

  val fillingInputConf = inputConf.copy(
    query = """select *, speed as "speed64" from SM_sparse_wide""",
    dataTransformation = Some(WideDataFilling(Map('speed -> 2000L, 'pos -> 2000L), None))
  )

  val rowSchema = NewRowSchema('series_storage, 'from, 'to, ('app, 1), 'id, 'subunit, 'uuid)

  val outputConf = JDBCOutputConf(
    "Test.SM_basic_patterns",
    rowSchema,
    s"jdbc:clickhouse://localhost:$jdbcPort/default",
    "ru.yandex.clickhouse.ClickHouseDriver"
  )

  val basicAssertions = Seq(
    RawPattern(1, "speed < 15"),
    RawPattern(3, "speed > 10.0", Some(Map("test" -> "test")), Some(540), Some(Seq('speed)))
  )
  val typesCasting = Seq(RawPattern(10, "speed = 15"), RawPattern(11, "speed64 < 15.0"))
  val filling = Seq(RawPattern(20, "speed = 20 and pos = 15"))

  override def afterStart(): Unit = {
    super.afterStart()

    Files
      .readResource("/sql/test-db-schema.sql")
      .mkString
      .split(";")
      .foreach(jdbcContainer.executeUpdate)

    Files
      .readResource("/sql/infl-test-db-schema.sql")
      .mkString
      .split(";")
      .foreach(influxContainer.executeQuery)

    Files
      .readResource("/sql/wide/infl-source-inserts.influx")
      .mkString
      .split(";")
      .foreach(influxContainer.executeUpdate)

    Files
      .readResource("/sql/sink-schema.sql")
      .mkString
      .split(";")
      .foreach(jdbcContainer.executeUpdate)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    container.stop()
  }

  "Basic assertions and forwarded fields" should "work for wide dense table" in {

    Post(
      "/streamJob/from-influxdb/to-jdbc/?run_async=0",
      FindPatternsRequest("1", inputConf, outputConf, basicAssertions)
    ) ~>
    route ~> check {
      status shouldEqual StatusCodes.OK

      // for 65001
      checkByQuery(
        List(List(2.0)),
        "SELECT toUnixTimestamp(to) - toUnixTimestamp(from) FROM Test.SM_basic_patterns WHERE id = 1 "
      )

      checkByQuery(
        List(List(1.0), List(1.0)),
        "SELECT toUnixTimestamp(to) - toUnixTimestamp(from) FROM Test.SM_basic_patterns WHERE id = 3 "
      )

    }
  }

  "Types casting" should "work for wide dense table" in {
    Post(
      "/streamJob/from-influxdb/to-jdbc/?run_async=0",
      FindPatternsRequest("2", typeCastingInputConf, outputConf, typesCasting)
    ) ~>
    route ~> check {
      status shouldEqual StatusCodes.OK

      // for 65001
      checkByQuery(
        List(List(0.0)),
        "SELECT toUnixTimestamp(to) - toUnixTimestamp(from) FROM Test.SM_basic_patterns WHERE id = 10 "
      )

      // for 65001 and 65002
      checkByQuery(
        List(List(2.0), List(0.0)),
        "SELECT toUnixTimestamp(to) - toUnixTimestamp(from) FROM Test.SM_basic_patterns WHERE id = 11 "
      )
    }
  }

  /*
  // TODO: Fix json format for arbitrary
  "Data filling" should "work for wide sparse table" in {

    Post(
      "/streamJob/from-influxdb/to-jdbc/?run_async=0",
      FindPatternsRequest("3", fillingInputConf, outputConf, filling)
    ) ~>
      route ~> check {
      status shouldEqual StatusCodes.OK

      checkByQuery(
        List(List(0.0)),
        "SELECT toUnixTimestamp(to) - toUnixTimestamp(from) FROM Test.SM_basic_patterns WHERE id = 20 "
      )
    }
  }
  **/
}
