package ru.itclover.tsp.http

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import com.dimafeng.testcontainers.ForAllTestContainer
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.types.Row
import org.scalatest.FlatSpec
import ru.itclover.tsp.core.RawPattern
import ru.itclover.tsp.http.domain.input.FindPatternsRequest
import ru.itclover.tsp.http.utils.{JDBCContainer, SqlMatchers}
import ru.itclover.tsp.io.input.{JDBCInputConf, NarrowDataUnfolding}
import ru.itclover.tsp.io.output.{JDBCOutputConf, RowSchema}
import ru.itclover.tsp.utils.Files

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.DurationInt

class NarrowTableTest extends FlatSpec with SqlMatchers with ScalatestRouteTest with HttpService with ForAllTestContainer {

  implicit override val executionContext: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global
  implicit override val streamEnvironment: StreamExecutionEnvironment =
    StreamExecutionEnvironment.createLocalEnvironment()
  streamEnvironment.setMaxParallelism(30000) // For proper keyBy partitioning

  implicit def defaultTimeout(implicit system: ActorSystem) = RouteTestTimeout(300.seconds)

  val port = 8151
  implicit override val container = new JDBCContainer(
    "yandex/clickhouse-server:latest",
    port -> 8123 :: 9088 -> 9000 :: Nil,
    "ru.yandex.clickhouse.ClickHouseDriver",
    s"jdbc:clickhouse://localhost:$port/default"
  )

  val transformation = NarrowDataUnfolding[Row, Symbol, Any]('key, 'value, Map('speed1 -> 1000, 'speed2 -> 1000))

  val inputConf = JDBCInputConf(
    sourceId = 123,
    jdbcUrl = container.jdbcUrl,
    query = """select * from Test.SM_basic_narrow""", // speed(1)(2) fancy colnames test
    driverName = container.driverName,
    datetimeField = 'datetime,
    eventsMaxGapMs = 60000L,
    defaultEventsGapMs = 1000L,
    chunkSizeMs = Some(900000L),
    partitionFields = Seq('series_id, 'mechanism_id),
    dataTransformation = Some(transformation)
  )

  val rowSchema = RowSchema('series_storage, 'from, 'to, ('app, 1), 'id, 'timestamp, 'context, inputConf.partitionFields)

  val outputConf = JDBCOutputConf(
    "Test.SM_basic_wide_patterns",
    rowSchema,
    s"jdbc:clickhouse://localhost:$port/default",
    "ru.yandex.clickhouse.ClickHouseDriver"
  )

  val basicAssertions = Seq(
    RawPattern("1", "speed1 < 15"),
    RawPattern("2", """"speed2" > 10""")
  )

  override def afterStart(): Unit = {
    super.afterStart()
    Files.readResource("/sql/test-db-schema.sql").mkString.split(";").map(container.executeUpdate)
    Files.readResource("/sql/narrow/source-schema.sql").mkString.split(";").map(container.executeUpdate)
    Files.readResource("/sql/narrow/source-inserts.sql").mkString.split(";").map(container.executeUpdate)
    Files.readResource("/sql/sink-schema.sql").mkString.split(";").map(container.executeUpdate)
  }

  "Basic assertions and forwarded fields" should "work for wide dense table" in {
    Post("/streamJob/from-jdbc/to-jdbc/?run_async=0", FindPatternsRequest("1", inputConf, outputConf, basicAssertions)) ~>
      route ~> check {
      entityAs[String] shouldBe ""
      status shouldEqual StatusCodes.OK
    }
  }
}
