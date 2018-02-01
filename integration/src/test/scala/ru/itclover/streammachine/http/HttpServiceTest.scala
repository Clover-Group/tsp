package ru.itclover.streammachine.http

import java.sql.{Connection, DriverManager, Time}
import java.util

import akka.actor.ActorSystem
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import akka.http.scaladsl.model.StatusCodes
import akka.stream.ActorMaterializer
import com.dimafeng.testcontainers
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.scalatest.{FlatSpec, FunSuite, Matchers, WordSpec}
import ru.itclover.streammachine.http.domain.input.FindPatternsRequest
import ru.itclover.streammachine.http.domain.output.SuccessfulResponse
import ru.itclover.streammachine.io.input.{JDBCInputConf, JDBCNarrowInputConf}
import ru.itclover.streammachine.io.input.source.JDBCSourceInfo
import ru.itclover.streammachine.io.output.JDBCOutputConf
import ru.itclover.streammachine.io.output.JDBCSegmentsSink

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.DurationInt
import com.dimafeng.testcontainers._
import org.testcontainers.containers.GenericContainer
import ru.itclover.streammachine.http.utils.JDBCContainer
import ru.itclover.streammachine.utils.Files

import scala.util.{Failure, Success, Try}


class HttpServiceTest extends FlatSpec with Matchers with ScalatestRouteTest with HttpService with ForAllTestContainer {
  override implicit val executionContext: ExecutionContextExecutor = scala.concurrent.ExecutionContext.Implicits.global
  override implicit val streamEnvironment: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment()

  implicit def defaultTimeout(implicit system: ActorSystem) = RouteTestTimeout(300.seconds)

  val port = 8125

  override val container = new JDBCContainer("yandex/clickhouse-server:latest", port -> 8123 :: 9000 -> 9000 :: Nil,
    "ru.yandex.clickhouse.ClickHouseDriver", s"jdbc:clickhouse://localhost:$port/default")

  val inputConf = JDBCInputConf(
    jdbcUrl = container.jdbcUrl,
    query = "select datetime, mechanism_id, speed from SM_Integration_wide limit 1000",
    driverName = container.driverName,
    datetimeColname = 'datetime,
    partitionColnames = Seq('mechanism_id)
  )
  // ...
  val narrowInputConf = JDBCNarrowInputConf(
    inputConf.copy(query = "select datetime, mechanism_id, sensor, value_float from SM_Integration_narrow limit 1000"),
    'sensor, 'value_float,
    Map('speed -> 2000)
  )

  val sinkSchema = JDBCSegmentsSink("SM_Integration_wide_patterns", 'from, 'from_millis, 'to, 'to_millis, 'pattern_id,
    inputConf.partitionColnames)
  val narrowSinkSchema = JDBCSegmentsSink("SM_Integration_narrow_patterns", 'from, 'from_millis, 'to, 'to_millis, 'pattern_id,
    inputConf.partitionColnames)
  val outputConf = JDBCOutputConf(s"jdbc:clickhouse://localhost:$port/default", sinkSchema,
    "ru.yandex.clickhouse.ClickHouseDriver")
  val patterns = Map("1" -> "'speed < 15.0", "2" -> "'speed > 10.0")

  override def afterStart(): Unit = {
    super.beforeAll()
    Files.readResource("/sql/test-db-schema.sql").mkString.split(";").map(container.executeUpdate)
    Files.readResource("/sql/wide/source-schema.sql").mkString.split(";").map(container.executeUpdate)
    Files.readResource("/sql/wide/source-inserts.sql").mkString.split(";").map(container.executeUpdate)
    Files.readResource("/sql/wide/sink-schema.sql").mkString.split(";").map(container.executeUpdate)
  }

  "Streaming patterns search" should "work for wide dense table" in {

    Post("/streaming/find-patterns/wide-dense-table/", FindPatternsRequest(inputConf, outputConf, patterns)) ~>
      route ~> check {
      status shouldEqual StatusCodes.OK

      checkSegments(2 :: Nil, "SELECT from, to FROM SM_Integration_wide_patterns WHERE pattern_id = '1'")
      checkSegments(3 :: Nil, "SELECT from, to FROM SM_Integration_wide_patterns WHERE pattern_id = '2'")
    }
  }

  "Streaming patterns search" should "work for narrow table" in {
    Post("/streaming/find-patterns/narrow-table/",
         FindPatternsRequest(narrowInputConf, outputConf.copy(sinkSchema=narrowSinkSchema), patterns)) ~>
      route ~> check {
      status shouldEqual StatusCodes.OK

      checkSegments(2 :: Nil, "SELECT from, to FROM SM_Integration_narrow_patterns WHERE pattern_id = '1'")
      checkSegments(3 :: Nil, "SELECT from, to FROM SM_Integration_narrow_patterns WHERE pattern_id = '2'")
    }
  }

  /** Util for checking segments count and size in seconds */
  def checkSegments(expectedSegmentsCounts: Seq[Int], query: String): Unit = {
    val resultSet = container.executeQuery(query)
    for (expectedSeconds <- expectedSegmentsCounts) {
      resultSet.next() shouldEqual true
      val from = resultSet.getTime(1)
      val to: Time = resultSet.getTime(2)
      val segmentSeconds = to.toLocalTime.getSecond - from.toLocalTime.getSecond
      segmentSeconds should === (expectedSeconds)
    }
  }
}

