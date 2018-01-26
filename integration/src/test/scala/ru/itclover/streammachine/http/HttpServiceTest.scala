package ru.itclover.streammachine.http

import java.sql.{Connection, DriverManager, Time}
import java.util

import akka.actor.ActorSystem
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import akka.http.scaladsl.model.StatusCodes
import com.dimafeng.testcontainers
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.scalatest.{FlatSpec, FunSuite, Matchers, WordSpec}
import ru.itclover.streammachine.http.domain.input.FindPatternsRequest
import ru.itclover.streammachine.http.domain.output.SuccessfulResponse
import ru.itclover.streammachine.io.input.JDBCInputConfig
import ru.itclover.streammachine.io.input.source.JDBCSourceInfo
import ru.itclover.streammachine.io.output.JDBCOutputConfig
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

  val port = 8124

  override val container = new JDBCContainer("yandex/clickhouse-server:latest", port -> 8123 :: 9000 -> 9000 :: Nil,
    "ru.yandex.clickhouse.ClickHouseDriver", s"jdbc:clickhouse://localhost:$port/default")

  var connection: Connection = _

  val inputConf = JDBCInputConfig(
    jdbcUrl = container.jdbcUrl,
    query = "select datetime, mechanism_id, speed from SM_Integration_wide limit 1000",
    driverName = container.driverName,
    datetimeColname = 'datetime,
    partitionColnames = Seq('mechanism_id)
  )
  val sinkSchema = JDBCSegmentsSink("SM_Integration_wide_patterns", 'from, 'from_millis, 'to, 'to_millis, 'pattern_id,
    inputConf.partitionColnames)
  val outputConf = JDBCOutputConfig(s"jdbc:clickhouse://localhost:$port/default", sinkSchema,
    "ru.yandex.clickhouse.ClickHouseDriver")
  val patterns = Map("1" -> "Assert[Row](event => event.getField(2).asInstanceOf[Float].toDouble > 10)")

  override def afterStart(): Unit = {
    super.beforeAll()
    connection = {
      Class.forName(container.driverName)
      DriverManager.getConnection(container.jdbcUrl)
    }
    connection.createStatement().executeUpdate(Files.readResource("/sql/test-db-schema.sql").mkString)
    connection.createStatement().executeUpdate(Files.readResource("/sql/wide/source-schema.sql").mkString)
    connection.createStatement().executeUpdate(Files.readResource("/sql/wide/source-inserts.sql").mkString)
    connection.createStatement().executeUpdate(Files.readResource("/sql/wide/sink-schema.sql").mkString)
  }

  "Streaming patterns search" should "work for wide table" in {

      Post("/streaming/find-patterns/wide-dense-table/", FindPatternsRequest(inputConf, outputConf, patterns)) ~>
        route ~> check {
        status shouldEqual StatusCodes.OK
        val resultSet = connection.createStatement()
          .executeQuery("SELECT from, to FROM SM_Integration_wide_patterns WHERE pattern_id = '1'")
        resultSet.next() shouldEqual true
        val from = resultSet.getTime(1)
        val to: Time = resultSet.getTime(2)
        val segmentSeconds = to.toLocalTime.getSecond - from.toLocalTime.getSecond
        segmentSeconds should ===(2)
      }

  }
}

