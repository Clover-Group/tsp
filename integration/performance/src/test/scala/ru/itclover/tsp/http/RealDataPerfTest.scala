package ru.itclover.tsp.http

import akka.http.scaladsl.model.StatusCodes
import com.dimafeng.testcontainers._
import com.typesafe.scalalogging.Logger
import org.scalatest.FlatSpec
import org.testcontainers.containers.wait.strategy.Wait
import ru.itclover.tsp.core.RawPattern
import ru.itclover.tsp.http.domain.input.FindPatternsRequest
import ru.itclover.tsp.http.domain.output.SuccessfulResponse.FinishedJobResponse
import ru.itclover.tsp.http.utils.{HttpServiceMathers, JDBCContainer}
import ru.itclover.tsp.io.input.JDBCInputConf
import ru.itclover.tsp.io.output.{JDBCOutputConf, NewRowSchema}
import ru.itclover.tsp.utils.Files

import scala.util.Success

class RealDataPerfTest extends FlatSpec with HttpServiceMathers with ForAllTestContainer {

  override val log = Logger("RealDataPerfTest")

  val port = 8136

  implicit override val container = new JDBCContainer(
    "yandex/clickhouse-server:latest",
    port -> 8123 :: 9087 -> 9000 :: Nil,
    "ru.yandex.clickhouse.ClickHouseDriver",
    s"jdbc:clickhouse://localhost:$port/default",
    waitStrategy = Some(Wait.forHttp("/").forStatusCode(200).forStatusCode(400))
  )

  val inputConf = JDBCInputConf(
    sourceId = 123,
    jdbcUrl = container.jdbcUrl,
    query = "select * from Test.Bigdata_HI limit 10000000",
    driverName = container.driverName,
    datetimeField = 'dt,
    eventsMaxGapMs = Some(60000L),
    defaultEventsGapMs = Some(10000L),
    chunkSizeMs = Some(900000L),
    partitionFields = Seq('stock_num)
  )

  val sinkSchema = NewRowSchema('series_storage, 'from, 'to, ('app, 1), 'id, 'subunit)

  val outputConf = JDBCOutputConf(
    "Test.SM_basic_patterns",
    sinkSchema,
    s"jdbc:clickhouse://localhost:$port/default",
    "ru.yandex.clickhouse.ClickHouseDriver"
  )

  val (realDataMaxTimeSec, realDataPatterns) = 30.0 -> Seq(
    RawPattern("6", "HI__wagon_id__6 < 0.5"),
    RawPattern("4", "HI__wagon_id__4 < 0.5")
  )

  override def afterStart(): Unit = {
    super.afterStart()

    Files.readResource("/sql/test-db-schema.sql")
         .mkString
         .split(";")
         .foreach(container.executeUpdate)

    Files.readResource("/sql/wide/bigdata-schema.sql")
         .mkString
         .split(";")
         .foreach(container.executeUpdate)

    val csvData = Files.readResource("/sql/wide/source_bigdata.csv")
                       .drop(1)
                       .mkString("\n")

    container.executeUpdate(s"INSERT INTO Test.Bigdata_HI FORMAT CSV\n${csvData}")

    Files.readResource("/sql/wide/sink-schema.sql")
         .mkString
         .split(";")
         .foreach(container.executeUpdate)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    container.stop()
  }

  "Basic assertions" should "work for wide dense table" in {

    Post("/streamJob/from-jdbc/to-jdbc/?run_async=0", FindPatternsRequest("1", inputConf, outputConf, realDataPatterns)) ~>
    route ~> check {
      status shouldEqual StatusCodes.OK
      val resp = unmarshal[FinishedJobResponse](responseEntity)
      resp shouldBe a[Success[_]]
      val execTimeS = resp.get.response.execTimeSec
      log.info(s"Test job completed for $execTimeS sec.")

      // Correctness
      checkByQuery(686.0 :: Nil, "SELECT count(*) FROM Test.SM_basic_patterns WHERE id = 6")
      checkByQuery(1078.0 :: Nil, "SELECT count(*) FROM Test.SM_basic_patterns WHERE id = 4")
      // Performance
      execTimeS should be <= realDataMaxTimeSec
    }
  }
}
