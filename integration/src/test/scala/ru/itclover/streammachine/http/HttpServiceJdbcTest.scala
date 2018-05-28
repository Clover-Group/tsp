package ru.itclover.streammachine.http

import akka.actor.ActorSystem
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import akka.http.scaladsl.model.StatusCodes
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.scalatest.{FlatSpec, Matchers}
import ru.itclover.streammachine.http.domain.input.FindPatternsRequest
import ru.itclover.streammachine.io.input.{JDBCInputConf, RawPattern}
import ru.itclover.streammachine.io.output.{JDBCOutputConf, RowSchema}
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.DurationInt
import com.dimafeng.testcontainers._
import ru.itclover.streammachine.http.utils.{JDBCContainer, SqlMatchers}
import ru.itclover.streammachine.utils.Files


class HttpServiceJdbcTest extends FlatSpec with SqlMatchers with ScalatestRouteTest with HttpService with ForAllTestContainer {
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
    query = "select * from Test.SM_basic_wide",
    driverName = container.driverName,
    datetimeFieldName = 'datetime,
    eventsMaxGapMs = 60000L,
    partitionFieldNames = Seq('series_id, 'mechanism_id)
  )

  val typeCastingInputConf = inputConf.copy(query = "select * from Test.SM_typeCasting_wide limit 1000")

  val rowSchema = RowSchema('series_storage, 'from,  'to, ('app, 1), 'id, 'timestamp, 'context,
    inputConf.partitionFieldNames)
  val outputConf = JDBCOutputConf("Test.SM_basic_wide_patterns", rowSchema, s"jdbc:clickhouse://localhost:$port/default",
    "ru.yandex.clickhouse.ClickHouseDriver")

  val basicAssertions = Seq(
    RawPattern("1", "Assert('speed.field < 15.0)"),
    RawPattern("2", "Assert('speed.field > 10.0)"),
    RawPattern("3", "Assert('speed.field > 10.0)", Map("test" -> "test"), Seq('speed)))
  val typesCasting = Seq(
    RawPattern("10", "Assert('speed.as[String] === \"15\" and 'speed.as[Int] === 15)"),
    RawPattern("11", "Assert('speed.as[Int] < 15)"),
    RawPattern("12", "Assert('speed64.as[Double] < 15.0)"),
    RawPattern("13", "Assert('speed64.field < 15.0)"))


  override def afterStart(): Unit = {
    super.afterStart()
    Files.readResource("/sql/test-db-schema.sql").mkString.split(";").map(container.executeUpdate)
    Files.readResource("/sql/wide/source-schema.sql").mkString.split(";").map(container.executeUpdate)
    Files.readResource("/sql/wide/source-inserts.sql").mkString.split(";").map(container.executeUpdate)
    Files.readResource("/sql/wide/sink-schema.sql").mkString.split(";").map(container.executeUpdate)
  }

  "Basic assertions and forwarded fields" should "work for wide dense table" in {

    Post("/streamJob/from-jdbc/to-jdbc/", FindPatternsRequest(inputConf, outputConf, basicAssertions)) ~>
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
    Post("/streamJob/from-jdbc/to-jdbc/", FindPatternsRequest(typeCastingInputConf, outputConf, typesCasting)) ~>
        route ~> check {
      status shouldEqual StatusCodes.OK

      checkByQuery(0 :: Nil, "SELECT to - from FROM Test.SM_basic_wide_patterns WHERE id = 10 AND " +
        "visitParamExtractString(context, 'mechanism_id') = '65001'")
      checkByQuery(2 :: Nil, "SELECT to - from FROM Test.SM_basic_wide_patterns WHERE id = 11 AND " +
        "visitParamExtractString(context, 'mechanism_id') = '65001'")
      checkByQuery(2 :: Nil, "SELECT to - from FROM Test.SM_basic_wide_patterns WHERE id = 12 AND " +
        "visitParamExtractString(context, 'mechanism_id') = '65001'")
      checkByQuery(2 :: Nil, "SELECT to - from FROM Test.SM_basic_wide_patterns WHERE id = 13 AND " +
        "visitParamExtractString(context, 'mechanism_id') = '65001'")
    }
  }
}

