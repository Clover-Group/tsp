package ru.itclover.tsp.http

import akka.actor.ActorSystem
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import akka.http.scaladsl.model.StatusCodes
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.scalatest.{FlatSpec, FunSuite, Matchers, WordSpec}
import ru.itclover.tsp.http.domain.input.FindPatternsRequest
import ru.itclover.tsp.io.input.{JDBCInputConf, RawPattern}
import ru.itclover.tsp.io.output.{JDBCOutputConf, RowSchema}
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.DurationInt
import com.dimafeng.testcontainers._
import com.typesafe.scalalogging.Logger
import ru.itclover.tsp.http.domain.output.FinishedJobResponse
import ru.itclover.tsp.http.utils.{JDBCContainer, RangeMatchers, SqlMatchers}
import ru.itclover.tsp.utils.Files
import scala.util.Success

class RealDataTest
    extends FlatSpec
    with SqlMatchers
    with ScalatestRouteTest
    with HttpService
    with ForAllTestContainer {

  implicit def defaultTimeout(implicit system: ActorSystem) = RouteTestTimeout(300.seconds)
  implicit override val executionContext: ExecutionContextExecutor = scala.concurrent.ExecutionContext.Implicits.global
  implicit override val streamEnvironment: StreamExecutionEnvironment =
    StreamExecutionEnvironment.createLocalEnvironment()
  streamEnvironment.setMaxParallelism(30000) // For proper keyBy partitioning


  private val log = Logger("RealDataTest")

  val port = 8136

  implicit override val container = new JDBCContainer(
    "yandex/clickhouse-server:latest",
    port -> 8123 :: 9087 -> 9000 :: Nil,
    "ru.yandex.clickhouse.ClickHouseDriver",
    s"jdbc:clickhouse://localhost:$port/default"
  )

  val inputConf = JDBCInputConf(
    sourceId = 123,
    jdbcUrl = container.jdbcUrl,
    query = "select * from Test.Bigdata_HI",
    driverName = container.driverName,
    datetimeField = 'dt,
    eventsMaxGapMs = 60000L,
    defaultEventsGapMs = 10000L,
    partitionFields = Seq('stock_num)
  )

  val sinkSchema =
    RowSchema('series_storage, 'from, 'to, ('app, 1), 'id, 'timestamp, 'context, inputConf.partitionFields)

  val outputConf = JDBCOutputConf(
    "Test.SM_basic_wide_patterns",
    sinkSchema,
    s"jdbc:clickhouse://localhost:$port/default",
    "ru.yandex.clickhouse.ClickHouseDriver"
  )

  val (timeRangeSec, assertions) = (1 to 30) -> Seq(
    RawPattern("6", "HI__wagon_id__6 < 0.5"),
    RawPattern("4", "HI__wagon_id__4 < 0.5")
  )

  override def afterStart(): Unit = {
    super.beforeAll()
    Files.readResource("/sql/test-db-schema.sql").mkString.split(";").map(container.executeUpdate)
    Files.readResource("/sql/wide/source_bigdata_HI_115k.sql").mkString.split(";").map(container.executeUpdate)
    Files.readResource("/sql/wide/sink-schema.sql").mkString.split(";").map(container.executeUpdate)
  }

  "Basic assertions" should "work for wide dense table" in {

    Post("/streamJob/from-jdbc/to-jdbc/?run_async=0", FindPatternsRequest("1", inputConf, outputConf, assertions)) ~>
    route ~> check {
      status shouldEqual StatusCodes.OK
      val resp = unmarshal[FinishedJobResponse](responseEntity)
      resp shouldBe a[Success[_]]
      val execTimeS = resp.get.response.execTimeSec
      log.info(s"Test job completed for $execTimeS sec.")

      // Correctness
      checkByQuery(1275 :: Nil, "SELECT count(*) FROM Test.SM_basic_wide_patterns WHERE id = 6")
      checkByQuery(1832 :: Nil, "SELECT count(*) FROM Test.SM_basic_wide_patterns WHERE id = 4")

      // Performance
      val fromT = timeRangeSec.head.toLong
      val toT = timeRangeSec.last.toLong
      execTimeS should (be >= fromT and be <= toT)
    }
  }
}
