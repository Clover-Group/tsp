package ru.itclover.tsp.http

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import com.dimafeng.testcontainers._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.scalatest.FlatSpec
import ru.itclover.tsp.core.RawPattern
import ru.itclover.tsp.http.domain.input.FindPatternsRequest
import ru.itclover.tsp.http.utils.{InfluxDBContainer, JDBCContainer, RangeMatchers, SqlMatchers}
import ru.itclover.tsp.io.input.{InfluxDBInputConf, JDBCInputConf, WideDataFilling}
import ru.itclover.tsp.io.output.{JDBCOutputConf, RowSchema}
import ru.itclover.tsp.utils.Files
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.DurationInt

class BasicInfluxToJdbcTest
    extends FlatSpec
    with SqlMatchers
    with ScalatestRouteTest
    with HttpService
    with ForAllTestContainer {

  implicit override val executionContext: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global
  implicit override val streamEnvironment: StreamExecutionEnvironment =
    StreamExecutionEnvironment.createLocalEnvironment()
  streamEnvironment.setMaxParallelism(30000) // For proper keyBy partitioning

  implicit def defaultTimeout(implicit system: ActorSystem) = RouteTestTimeout(300.seconds)

  val influxPort = 8138

  val influxContainer =
    new InfluxDBContainer("influxdb:1.5", influxPort -> 8086 :: Nil, s"http://localhost:$influxPort", "Test", "default")

  val jdbcPort = 8157
  implicit val jdbcContainer = new JDBCContainer(
    "yandex/clickhouse-server:latest",
    jdbcPort -> 8123 :: 9072 -> 9000 :: Nil,
    "ru.yandex.clickhouse.ClickHouseDriver",
    s"jdbc:clickhouse://localhost:$jdbcPort/default"
  )

  override val container = MultipleContainers(LazyContainer(jdbcContainer), LazyContainer(influxContainer))

  val inputConf = InfluxDBInputConf(
    sourceId = 123,
    url = influxContainer.url,
    query = "select * from SM_basic_wide",
    dbName = influxContainer.dbName,
    eventsMaxGapMs = 60000L,
    defaultEventsGapMs = 1000L,
    partitionFields = Seq('series_id, 'mechanism_id),
    userName = Some("default")
  )
  val typeCastingInputConf = inputConf.copy(query = """select *, speed as "speed(1)(2)" from SM_typeCasting_wide""")
  val fillingInputConf = inputConf.copy(query = """select * from SM_sparse_wide""",
    dataTransformation = Some(WideDataFilling(Map(0 -> 2000L, 1 -> 2000L), None)))

  val rowSchema = RowSchema('series_storage, 'from, 'to, ('app, 1), 'id, 'timestamp, 'context, inputConf.partitionFields)

  val outputConf = JDBCOutputConf(
    "Test.SM_basic_wide_patterns",
    rowSchema,
    s"jdbc:clickhouse://localhost:$jdbcPort/default",
    "ru.yandex.clickhouse.ClickHouseDriver"
  )

  val basicAssertions = Seq(
    RawPattern("1", "speed < 15"),
    RawPattern("3", "speed > 10.0", Map("test" -> "test"), Seq('speed))
  )
  val typesCasting = Seq(RawPattern("10", "speed = 15"), RawPattern("11", "speed64 < 15.0"))
  val filling = Seq(RawPattern("20", "speed = 20 and pos = 15"))

  override def afterStart(): Unit = {
    super.afterStart()
    Files.readResource("/sql/test-db-schema.sql").mkString.split(";").map(jdbcContainer.executeUpdate)
    Files.readResource("/sql/infl-test-db-schema.sql").mkString.split(";").foreach(influxContainer.executeQuery)
    Files.readResource("/sql/wide/infl-source-inserts.influx").mkString.split(";").foreach(influxContainer.executeUpdate)
    Files.readResource("/sql/wide/sink-schema.sql").mkString.split(";").map(jdbcContainer.executeUpdate)
  }

  "Basic assertions and forwarded fields" should "work for wide dense table" in {

    Post(
      "/streamJob/from-influxdb/to-jdbc/?run_async=0",
      FindPatternsRequest("1", inputConf, outputConf, basicAssertions)
    ) ~>
    route ~> check {
      status shouldEqual StatusCodes.OK

      checkByQuery(
        2 :: Nil,
        "SELECT to - from FROM Test.SM_basic_wide_patterns WHERE id = 1 and " +
        "visitParamExtractString(context, 'mechanism_id') = '65001'"
      )
      checkByQuery(
        1 :: Nil,
        "SELECT to - from FROM Test.SM_basic_wide_patterns WHERE id = 3 and " +
        "visitParamExtractString(context, 'mechanism_id') = '65001' and visitParamExtractFloat(context, 'speed') = 20.0"
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

      checkByQuery(
        0 :: Nil,
        "SELECT to - from FROM Test.SM_basic_wide_patterns WHERE id = 10 AND " +
        "visitParamExtractString(context, 'mechanism_id') = '65001'"
      )
      checkByQuery(
        2 :: Nil,
        "SELECT to - from FROM Test.SM_basic_wide_patterns WHERE id = 11 AND " +
        "visitParamExtractString(context, 'mechanism_id') = '65001'"
      )
    }
  }

  // TODO: Fix json format for arbitrary
  /*"Data filling" should "work for wide sparse table" in {

    Post(
      "/streamJob/from-influxdb/to-jdbc/?run_async=0",
      FindPatternsRequest("3", fillingInputConf, outputConf, filling)
    ) ~>
      route ~> check {
      status shouldEqual StatusCodes.OK

      checkByQuery(
        0.0 :: Nil,
        "SELECT to - from FROM Test.SM_basic_wide_patterns WHERE id = 20 AND " +
          "visitParamExtractString(context, 'mechanism_id') = '65001'"
      )
    }
  }*/
}
