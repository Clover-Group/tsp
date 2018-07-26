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


class HttpServiceInfluxToJdbcTest extends FlatSpec with SqlMatchers with ScalatestRouteTest
  with HttpService with ForAllTestContainer
{
  override implicit val executionContext: ExecutionContextExecutor = scala.concurrent.ExecutionContext.Implicits.global
  override implicit val streamEnvironment: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment()
  streamEnvironment.setMaxParallelism(30000) // For proper keyBy partitioning

  implicit def defaultTimeout(implicit system: ActorSystem) = RouteTestTimeout(300.seconds)

  val influxPort = 8138
  val influxContainer = new InfluxDBContainer("influxdb:1.5", influxPort -> 8086 :: Nil,
    s"http://localhost:$influxPort", "Test", "default")

  val jdbcPort = 8156
  implicit val jdbcContainer = new JDBCContainer("yandex/clickhouse-server:latest", jdbcPort -> 8123 :: 9072 -> 9000 :: Nil,
    "ru.yandex.clickhouse.ClickHouseDriver", s"jdbc:clickhouse://localhost:$jdbcPort/default")

  override val container = MultipleContainers(LazyContainer(jdbcContainer), LazyContainer(influxContainer))

  val inputConf = InfluxDBInputConf(
    sourceId = 123,
    url = influxContainer.url,
    query = "select * from SM_basic_wide",
    dbName = influxContainer.dbName,
    eventsMaxGapMs = 60000L,
    partitionFields = Seq('series_id, 'mechanism_id),
    userName = Some("default")
  )
  val typeCastingInputConf = inputConf.copy(query = "select * from SM_typeCasting_wide")

  val rowSchema = RowSchema('series_storage, 'from,  'to, ('app, 1), 'id, 'timestamp, 'context,
    inputConf.partitionFields)
  val outputConf = JDBCOutputConf("Test.SM_basic_wide_patterns", rowSchema, s"jdbc:clickhouse://localhost:$jdbcPort/default",
    "ru.yandex.clickhouse.ClickHouseDriver")

  val basicAssertions = Seq(
    RawPattern("1", "Assert('speed.field < 15.0)"),
    RawPattern("2", "Assert('speed.field > 10.0)"),
    RawPattern("3", "Assert('speed.field > 10.0)", Map("test" -> "test"), Seq('speed)))
  val typesCasting = Seq(
    // Int-extracting not working for now - influx client replace all with double
    // RawPattern("10", "Assert('speed.as[String] === \"15\" and 'speed.as[Int] === 15)"),
    // RawPattern("11", "Assert('speed.as[Int] < 15)"),
    RawPattern("12", "Assert('speed64.as[Double] < 15.0)"),
    RawPattern("13", "Assert('speed64.field < 15.0)"),
    RawPattern("14", "Assert('speed.as[String] === \"15.0\")"))

  override def afterStart(): Unit = {
    super.afterStart()
    Files.readResource("/sql/test-db-schema.sql").mkString.split(";").map(jdbcContainer.executeUpdate)
    Files.readResource("/sql/infl-test-db-schema.sql").mkString.split(";").foreach(influxContainer.executeQuery)
    Files.readResource("/sql/wide/infl-source-inserts.sql").mkString.split(";").foreach(influxContainer.executeUpdate)
    Files.readResource("/sql/wide/sink-schema.sql").mkString.split(";").map(jdbcContainer.executeUpdate)
  }

  "Basic assertions and forwarded fields" should "work for wide dense table" in {

    Post("/streamJob/from-influxdb/to-jdbc/", FindPatternsRequest("1", inputConf, outputConf, basicAssertions)) ~>
        route ~> check {
      status shouldEqual StatusCodes.OK

      checkByQuery(2 :: Nil, "SELECT to - from FROM Test.SM_basic_wide_patterns WHERE id = 1 and " +
        "visitParamExtractString(context, 'mechanism_id') = '65001'")

      checkByQuery(1 :: Nil, "SELECT to - from FROM Test.SM_basic_wide_patterns WHERE id = 2 and " +
        "visitParamExtractString(context, 'mechanism_id') = '65001'")
      checkByQuery(1 :: Nil, "SELECT to - from FROM Test.SM_basic_wide_patterns WHERE id = 2 and " +
        "visitParamExtractString(context, 'mechanism_id') = '65002'")

      checkByQuery(1 :: Nil, "SELECT to - from FROM Test.SM_basic_wide_patterns WHERE id = 3 and " +
        "visitParamExtractString(context, 'mechanism_id') = '65001' and visitParamExtractFloat(context, 'speed') = 20.0")
    }
  }

  "Types casting" should "work for wide dense table" in {
    Post("/streamJob/from-influxdb/to-jdbc/", FindPatternsRequest("2", typeCastingInputConf, outputConf, typesCasting)) ~>
        route ~> check {
      status shouldEqual StatusCodes.OK

      // Int-extracting not working for now - influx client replace all with double
      /*checkByQuery(0 :: Nil, "SELECT to - from FROM Test.SM_basic_wide_patterns WHERE id = 10 AND " +
        "visitParamExtractString(context, 'mechanism_id') = '65001'")
      checkByQuery(2 :: Nil, "SELECT to - from FROM Test.SM_basic_wide_patterns WHERE id = 11 AND " +
        "visitParamExtractString(context, 'mechanism_id') = '65001'")*/
      checkByQuery(2 :: Nil, "SELECT to - from FROM Test.SM_basic_wide_patterns WHERE id = 12 AND " +
        "visitParamExtractString(context, 'mechanism_id') = '65001'")
      checkByQuery(2 :: Nil, "SELECT to - from FROM Test.SM_basic_wide_patterns WHERE id = 13 AND " +
        "visitParamExtractString(context, 'mechanism_id') = '65001'")
      checkByQuery(0 :: Nil, "SELECT to - from FROM Test.SM_basic_wide_patterns WHERE id = 14 AND " +
        "visitParamExtractString(context, 'mechanism_id') = '65001'")
    }
  }
}

