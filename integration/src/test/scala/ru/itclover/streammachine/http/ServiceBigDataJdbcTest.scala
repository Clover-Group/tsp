package ru.itclover.streammachine.http

import akka.actor.ActorSystem
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import akka.http.scaladsl.model.StatusCodes
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.scalatest.{FlatSpec, FunSuite, Matchers, WordSpec}
import ru.itclover.streammachine.http.domain.input.FindPatternsRequest
import ru.itclover.streammachine.io.input.{JDBCInputConf, RawPattern}
import ru.itclover.streammachine.io.output.{JDBCOutputConf, RowSchema}
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.DurationInt
import com.dimafeng.testcontainers._
import ru.itclover.streammachine.http.utils.{JDBCContainer, SqlMatchers}
import ru.itclover.streammachine.utils.Files


class ServiceBigDataJdbcTest extends FlatSpec with SqlMatchers with ScalatestRouteTest with HttpService with ForAllTestContainer {
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
    datetimeFieldName = 'dt,
    eventsMaxGapMs = 60000L,
    partitionFieldNames = Seq('stock_num)
  )

  val sinkSchema = RowSchema('series_storage,
    'from,  'to, ('app, 1), 'id, 'timestamp, 'context,
    inputConf.partitionFieldNames)
  val outputConf = JDBCOutputConf("Test.SM_basic_wide_patterns", sinkSchema,
    s"jdbc:clickhouse://localhost:$port/default", "ru.yandex.clickhouse.ClickHouseDriver")

  val assertions = Seq(
    RawPattern("6", "Assert('HI__wagon_id__6.as[Double] < 0.5)"),
    RawPattern("4", "Assert('HI__wagon_id__4.as[Double] < 0.5)")
  )

  override def afterStart(): Unit = {
    super.beforeAll()
    Files.readResource("/sql/test-db-schema.sql").mkString.split(";").map(container.executeUpdate)
    Files.readResource("/sql/wide/source_bigdata_HI_115k.sql").mkString.split(";").map(container.executeUpdate)
    Files.readResource("/sql/wide/sink-schema.sql").mkString.split(";").map(container.executeUpdate)
  }

  "Basic assertions" should "work for wide dense table" in {

    Post("/streamJob/from-jdbc/to-jdbc/", FindPatternsRequest(inputConf, outputConf, assertions)) ~>
        route ~> check {
      status shouldEqual StatusCodes.OK

      checkByQuery(610 :: Nil, "SELECT count(*) FROM Test.SM_basic_wide_patterns WHERE id = 6")
      checkByQuery(996 :: Nil, "SELECT count(*) FROM Test.SM_basic_wide_patterns WHERE id = 4")
      checkByQuery(610 + 996 :: Nil, "SELECT count(*) FROM Test.SM_basic_wide_patterns WHERE id = 4 or id = 6")
    }
  }
}

