package ru.itclover.streammachine.http

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import com.dimafeng.testcontainers._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.scalatest.FlatSpec
import ru.itclover.streammachine.http.domain.input.FindPatternsRequest
import ru.itclover.streammachine.http.utils.{InfluxDBContainer, JDBCContainer, SqlMatchers}
import ru.itclover.streammachine.io.input.{InfluxDBInputConf, JDBCInputConf, RawPattern}
import ru.itclover.streammachine.io.output.{JDBCOutputConf, RowSchema}
import ru.itclover.streammachine.utils.Files

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.DurationInt


class HttpServiceInfluxToJdbcTest extends FlatSpec with SqlMatchers with ScalatestRouteTest with HttpService with ForAllTestContainer {
  override implicit val executionContext: ExecutionContextExecutor = scala.concurrent.ExecutionContext.Implicits.global
  override implicit val streamEnvironment: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment()
  streamEnvironment.setMaxParallelism(30000) // For proper keyBy partitioning

  implicit def defaultTimeout(implicit system: ActorSystem) = RouteTestTimeout(300.seconds)

  val port = 8138
  val influxContainer = new InfluxDBContainer("influxdb:1.5", port -> 8086 :: Nil,
    s"http://localhost:8086", "NOAA_water_database")

  val jdbcPort = 8155
  val jdbcContainer = new JDBCContainer("yandex/clickhouse-server:latest", jdbcPort -> 8123 :: 9072 -> 9000 :: Nil,
    "ru.yandex.clickhouse.ClickHouseDriver", s"jdbc:clickhouse://localhost:$jdbcPort/default")

  override val container = MultipleContainers(LazyContainer(jdbcContainer), LazyContainer(influxContainer))

  val inputConf = InfluxDBInputConf(
    sourceId = 123,
    url = influxContainer.url,
    query = "select max(water_level) as water_level, location from h2o_feet GROUP BY time(12m) fill(previous) limit 10000",
    dbName = influxContainer.dbName,
    datetimeField = 'time,
    eventsMaxGapMs = 60000L,
    partitionFields = Seq('location),
    userName = Some("clover"),
    password = Some("g29s7qkn")
  )

  val rowSchema = RowSchema('series_storage, 'from,  'to, ('app, 1), 'id, 'timestamp, 'context,
    inputConf.partitionFields)
  val outputConf = JDBCOutputConf("Test.SM_basic_wide_patterns", rowSchema, s"jdbc:clickhouse://localhost:$jdbcPort/default",
    "ru.yandex.clickhouse.ClickHouseDriver")

  val basicAssertions = Seq(
    RawPattern("1", "Assert('water_level.field > 1.0)"))

  override def afterStart(): Unit = {
    super.afterStart()
    Files.readResource("/sql/test-db-schema.sql").mkString.split(";").map(jdbcContainer.executeUpdate)
    /*Files.readResource("/sql/wide/source-schema.sql").mkString.split(";").map(jdbcContainer.executeUpdate)
    Files.readResource("/sql/wide/source-inserts.sql").mkString.split(";").map(jdbcContainer.executeUpdate)*/
    Files.readResource("/sql/wide/sink-schema.sql").mkString.split(";").map(jdbcContainer.executeUpdate)
  }

  "Basic assertions and forwarded fields" should "work for wide dense table" in {

    Post("/streamJob/from-influxdb/to-jdbc/", FindPatternsRequest(inputConf, outputConf, basicAssertions)) ~>
        route ~> check {
      status shouldEqual StatusCodes.OK
      // TODO
      /*checkByQuery(2 :: Nil, "SELECT to - from FROM Test.SM_basic_wide_patterns WHERE id = 1 and " +
        "visitParamExtractString(context, 'mechanism_id') = '65001'")

      checkByQuery(1 :: Nil, "SELECT to - from FROM Test.SM_basic_wide_patterns WHERE id = 2 and " +
        "visitParamExtractString(context, 'mechanism_id') = '65001'")
      checkByQuery(1 :: Nil, "SELECT to - from FROM Test.SM_basic_wide_patterns WHERE id = 2 and " +
        "visitParamExtractString(context, 'mechanism_id') = '65002'")

      checkByQuery(1 :: Nil, "SELECT to - from FROM Test.SM_basic_wide_patterns WHERE id = 3 and " +
        "visitParamExtractString(context, 'mechanism_id') = '65001' and visitParamExtractFloat(context, 'speed') = 20.0")*/
    }
  }
}

