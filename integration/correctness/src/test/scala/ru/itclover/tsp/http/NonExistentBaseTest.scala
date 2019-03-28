package ru.itclover.tsp.http

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.scalatest.FlatSpec
import ru.itclover.tsp.http.domain.input.FindPatternsRequest
import ru.itclover.tsp.http.utils.SqlMatchers
import ru.itclover.tsp.io.input.JDBCInputConf
import ru.itclover.tsp.io.output.{JDBCOutputConf, RowSchema}

import scala.concurrent.ExecutionContextExecutor

class NonExistentBaseTest
    extends FlatSpec
    with SqlMatchers
    with ScalatestRouteTest
    with HttpService {
  implicit override val executionContext: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global
  implicit override val streamEnvironment: StreamExecutionEnvironment =
    StreamExecutionEnvironment.createLocalEnvironment()

  val dummyPort = 6000

  val inputConf = JDBCInputConf(
    sourceId = 123,
    jdbcUrl = s"jdbc:clickhouse://localhost:$dummyPort/default",
    query = """select *, speed as "speed(1)(2)" from Test.SM_basic_wide""", // speed(1)(2) fancy colnames test
    driverName = "ru.yandex.clickhouse.ClickHouseDriver",
    datetimeField = 'datetime,
    eventsMaxGapMs = 60000L,
    defaultEventsGapMs = 1000L,
    chunkSizeMs = Some(900000L),
    partitionFields = Seq('series_id, 'mechanism_id)
  )

  val rowSchema = RowSchema('series_storage, 'from, 'to, ('app, 1), 'id, 'timestamp, 'context, inputConf.partitionFields)

  val outputConf = JDBCOutputConf(
    "Test.SM_basic_wide_patterns",
    rowSchema,
    s"jdbc:clickhouse://localhost:$dummyPort/default",
    "ru.yandex.clickhouse.ClickHouseDriver"
  )

  "Non-existent database" should "give error upon execution" in {
    Post("/streamJob/from-jdbc/to-jdbc/?run_async=0", FindPatternsRequest("1", inputConf, outputConf, Seq())) ~>
    route ~> check {
      status shouldEqual StatusCodes.BadRequest
    }
  }
}
