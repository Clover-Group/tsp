package ru.itclover.streammachine.http

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import com.dimafeng.testcontainers._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.scalatest.FlatSpec
import ru.itclover.streammachine.http.domain.input.FindPatternsRequest
import ru.itclover.streammachine.http.domain.output.FinishedJobResponse
import ru.itclover.streammachine.http.utils.{JDBCContainer, RangeMatchers, SqlMatchers}
import ru.itclover.streammachine.io.input.{JDBCInputConf, RawPattern}
import ru.itclover.streammachine.io.output.{JDBCOutputConf, RowSchema}
import ru.itclover.streammachine.utils.Files
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.DurationInt
import scala.util.Success


class AccumsPerformanceTest extends FlatSpec with SqlMatchers with RangeMatchers with ScalatestRouteTest
      with HttpService with ForAllTestContainer {
  override implicit val executionContext: ExecutionContextExecutor = scala.concurrent.ExecutionContext.Implicits.global
  override implicit val streamEnvironment: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment()
  streamEnvironment.setMaxParallelism(30000) // For proper keyBy partitioning

  implicit def defaultTimeout(implicit system: ActorSystem) = RouteTestTimeout(300.seconds)

  val port = 8136

  override implicit val container = new JDBCContainer("yandex/clickhouse-server:latest", port -> 8123 :: 9087 -> 9000 :: Nil,
    "ru.yandex.clickhouse.ClickHouseDriver", s"jdbc:clickhouse://localhost:$port/default")

  val inputConf = JDBCInputConf(
    sourceId = 123,
    jdbcUrl = container.jdbcUrl,
    query = "select * from Test.Bigdata_HI",
    driverName = container.driverName,
    datetimeField = 'dt,
    eventsMaxGapMs = 60000L,
    partitionFields = Seq('stock_num)
  )

  val sinkSchema = RowSchema('series_storage,
    'from,  'to, ('app, 1), 'id, 'timestamp, 'context,
    inputConf.partitionFields)
  val outputConf = JDBCOutputConf("Test.SM_basic_wide_patterns", sinkSchema,
    s"jdbc:clickhouse://localhost:$port/default", "ru.yandex.clickhouse.ClickHouseDriver")

  val (simpleMath, simpleMathTimeRange) = (Seq(
    RawPattern("0", "sum()"),
    RawPattern("1", "avg()")
  ), 2 to 15)
  val counts = Seq(
    RawPattern("10", "HI__wagon_id__6 < 0.5"),
    RawPattern("11", "HI__wagon_id__4 < 0.5")
  )
  val timeCounts = Seq(
    RawPattern("20", "HI__wagon_id__6 < 0.5"),
    RawPattern("21", "HI__wagon_id__4 < 0.5")
  )

  val expectedTimeRanges = Map(
    "0" -> (10 +- 8),
    "1" -> (10 +- 8),
    "10" -> (10 +- 8),
    "11" -> (10 +- 8),
    "20" -> (10 +- 8),
    "21" -> (3 +- 30)
  )

  override def afterStart(): Unit = {
    super.beforeAll()
    // TODO more data?
    Files.readResource("/sql/test-db-schema.sql").mkString.split(";").map(container.executeUpdate)
    Files.readResource("/sql/wide/source_bigdata_HI_115k.sql").mkString.split(";").map(container.executeUpdate)
    Files.readResource("/sql/wide/sink-schema.sql").mkString.split(";").map(container.executeUpdate)
  }

  "Simple math" should "compute in time" in {

    Post("/streamJob/from-jdbc/to-jdbc/?run_async=0", FindPatternsRequest("1", inputConf, outputConf, simpleMath)) ~>
        route ~> check {
      status shouldEqual StatusCodes.OK
      val resp = unmarshal[FinishedJobResponse](responseEntity)

      resp shouldBe a[Success[_]]
      resp.get shouldBe beWithin(simpleMathTimeRange)

      // TODO correctness checks
      // checkByQuery(610 :: Nil, "SELECT count(*) FROM Test.SM_basic_wide_patterns WHERE id = 6")
    }
  }
}

