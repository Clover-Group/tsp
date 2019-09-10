package ru.itclover.tsp.http

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import com.dimafeng.testcontainers._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.scalatest.FlatSpec
import ru.itclover.tsp.core.RawPattern
import ru.itclover.tsp.http.domain.input.FindPatternsRequest
import ru.itclover.tsp.http.utils.{JDBCContainer, SqlMatchers}
import ru.itclover.tsp.io.input.JDBCInputConf
import ru.itclover.tsp.io.output.{JDBCOutputConf, RowSchema}
import ru.itclover.tsp.utils.Files

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.DurationInt

class BasicJdbcTest extends FlatSpec with SqlMatchers with ScalatestRouteTest with HttpService with ForAllTestContainer {

  implicit override val executionContext: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global
  implicit override val streamEnvironment: StreamExecutionEnvironment =
    StreamExecutionEnvironment.createLocalEnvironment()
  streamEnvironment.setMaxParallelism(30000) // For proper keyBy partitioning

  implicit def defaultTimeout(implicit system: ActorSystem) = RouteTestTimeout(300.seconds)

  val port = 8148
  implicit override val container = new JDBCContainer(
    "yandex/clickhouse-server:latest",
    port -> 8123 :: 9087 -> 9000 :: Nil,
    "ru.yandex.clickhouse.ClickHouseDriver",
    s"jdbc:clickhouse://localhost:$port/default"
  )

  val inputConf = JDBCInputConf(
    sourceId = 123,
    jdbcUrl = container.jdbcUrl,
    query = """select *, speed as "speed(1)(2)" from Test.SM_basic_wide""", // speed(1)(2) fancy colnames test
    driverName = container.driverName,
    datetimeField = 'datetime,
    eventsMaxGapMs = 60000L,
    defaultEventsGapMs = 1000L,
    chunkSizeMs = Some(900000L),
    partitionFields = Seq('series_id, 'mechanism_id)
  )

  val typeCastingInputConf = inputConf.copy(query = "select * from Test.SM_typeCasting_wide limit 1000")

  val rowSchema = RowSchema('series_storage, 'from, 'to, ('app, 1), 'id, 'timestamp, 'context, inputConf.partitionFields)

  val outputConf = JDBCOutputConf(
    "Test.SM_basic_patterns",
    rowSchema,
    s"jdbc:clickhouse://localhost:$port/default",
    "ru.yandex.clickhouse.ClickHouseDriver"
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
    Files.readResource("/sql/test-db-schema.sql").mkString.split(";").map(container.executeUpdate)
    Files.readResource("/sql/wide/source-schema.sql").mkString.split(";").map(container.executeUpdate)
    Files.readResource("/sql/wide/source-inserts.sql").mkString.split(";").map(container.executeUpdate)
    Files.readResource("/sql/sink-schema.sql").mkString.split(";").map(container.executeUpdate)
  }

  "Basic assertions and forwarded fields" should "work for wide dense table" in {

    Post("/streamJob/from-jdbc/to-jdbc/?run_async=0", FindPatternsRequest("1", inputConf, outputConf, basicAssertions)) ~>
    route ~> check {
      status shouldEqual StatusCodes.OK

      checkByQuery(
        2 :: Nil,
        "SELECT to - from FROM Test.SM_basic_patterns WHERE id = 1 and " +
        "visitParamExtractString(context, 'mechanism_id') = '65001'"
      )

      checkByQuery(
        1 :: Nil,
        "SELECT to - from FROM Test.SM_basic_patterns WHERE id = 2 and " +
        "visitParamExtractString(context, 'mechanism_id') = '65001'"
      )
      checkByQuery(
        1 :: Nil,
        "SELECT to - from FROM Test.SM_basic_patterns WHERE id = 2 and " +
        "visitParamExtractString(context, 'mechanism_id') = '65002'"
      )

      checkByQuery(
        1 :: Nil,
        "SELECT to - from FROM Test.SM_basic_patterns WHERE id = 3 and " +
        "visitParamExtractString(context, 'mechanism_id') = '65001' and visitParamExtractFloat(context, 'speed') = 20.0"
      )
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
        0 :: Nil,
        "SELECT to - from FROM Test.SM_basic_patterns WHERE id = 10 AND " +
        "visitParamExtractString(context, 'mechanism_id') = '65001'"
      )
      checkByQuery(
        2 :: Nil,
        "SELECT to - from FROM Test.SM_basic_patterns WHERE id = 11 AND " +
        "visitParamExtractString(context, 'mechanism_id') = '65001'"
      )
    }
  }
}
