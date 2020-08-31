package ru.itclover.tsp.http

import akka.http.scaladsl.model.StatusCodes
import com.dimafeng.testcontainers._
import com.typesafe.scalalogging.Logger
import org.scalatest.FlatSpec
import org.testcontainers.containers.wait.strategy.Wait
import ru.itclover.tsp.http.domain.input.FindPatternsRequest
import ru.itclover.tsp.http.domain.output.SuccessfulResponse.FinishedJobResponse
import ru.itclover.tsp.http.utils.{HttpServiceMathers, JDBCContainer}
import ru.itclover.tsp.io.input.JDBCInputConf
import ru.itclover.tsp.io.output.JDBCOutputConf
import ru.itclover.tsp.utils.Files
import spray.json.JsonParser
import spray.json.ParserInput.StringBasedParserInput

import scala.util.Success

class DebugRealDataTest extends FlatSpec with HttpServiceMathers with ForAllTestContainer {

  override val log = Logger("RealDataPerfTest")

  val port = 8190

  implicit override val container = new JDBCContainer(
    "yandex/clickhouse-server:latest",
    port -> 8123 :: 9101 -> 9000 :: Nil,
    "ru.yandex.clickhouse.ClickHouseDriver",
    s"jdbc:clickhouse://localhost:$port/default",
    // HTTP port returns 200, native port returns 400
    waitStrategy = Some(Wait.forHttp("/").forStatusCode(200).forStatusCode(400))
  )

  val inputConf = JDBCInputConf(
    sourceId = 123,
    jdbcUrl = container.jdbcUrl,
    query = "",
    driverName = container.driverName,
    datetimeField = 'dt,
    eventsMaxGapMs = Some(60000L),
    defaultEventsGapMs = Some(10000L),
    chunkSizeMs = Some(900000L),
    partitionFields = Seq('stock_num)
  )

  val realDataMaxTimeSec = 300.0

  override def afterStart(): Unit = {
    super.afterStart()
    Files.readResource("/debug/create-table.sql").mkString.split(";").map(container.executeUpdate)

    import scala.collection.JavaConverters._
    val rootzip = new java.util.zip.ZipFile("integration/performance/src/test/resources/debug/data2.csv.zip")
    val inputCSV =
      rootzip.entries().asScala.flatMap(e => scala.io.Source.fromInputStream(rootzip.getInputStream(e)).getLines())

    val insertString = (Iterator("INSERT INTO ep2k_tmy_20190708_wide_rep FORMAT CSV") ++ inputCSV).mkString("\n")
    container.executeUpdate(insertString)

    Files.readResource("/debug/sink-schema.sql").mkString.split(";").map(container.executeUpdate)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    container.stop()
  }

  "Basic assertions" should "work for wide dense table" in {
    val requestString = Files.readResource("/debug/request.json").mkString

    val input = JsonParser(new StringBasedParserInput(requestString))
    val request: FindPatternsRequest[JDBCInputConf, JDBCOutputConf] =
      spray.json.jsonReader[FindPatternsRequest[JDBCInputConf, JDBCOutputConf]].read(input)

    // todo add checkpointing! https://tech.signavio.com/2017/postgres-flink-sink
    val finalRequest =
      request.copy(
        inputConf = request.inputConf.copy(jdbcUrl = inputConf.jdbcUrl),
        outConf = request.outConf.copy(
          jdbcUrl = s"jdbc:clickhouse://localhost:$port/default",
          driverName = "ru.yandex.clickhouse.ClickHouseDriver",
          password = None,
          userName = None
        )
      )

    Post("/streamJob/from-jdbc/to-jdbc/?run_async=0", finalRequest) ~>
    route ~> check {
      status shouldEqual StatusCodes.OK
      val resp = unmarshal[FinishedJobResponse](responseEntity)
      resp shouldBe a[Success[_]]
      val execTimeS = resp.get.response.execTimeSec
      log.info(s"Test job completed for $execTimeS sec.")

      // Correctness
      checkByQuery(860.0 :: Nil, "SELECT count(*) FROM events_ep2k")
      // Performance
      execTimeS should be <= realDataMaxTimeSec
    }
  }

}
