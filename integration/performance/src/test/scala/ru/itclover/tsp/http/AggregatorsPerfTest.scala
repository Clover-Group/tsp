package ru.itclover.tsp.http

import com.dimafeng.testcontainers._
import org.scalatest.FlatSpec
import ru.itclover.tsp.core.RawPattern
import ru.itclover.tsp.http.utils.{HttpServiceMathers, JDBCContainer}
import ru.itclover.tsp.io.input.JDBCInputConf
import ru.itclover.tsp.io.output.{JDBCOutputConf, RowSchema}
import ru.itclover.tsp.utils.Files

class AggregatorsPerfTest extends FlatSpec with HttpServiceMathers with ForAllTestContainer {

  val port = 8136
  implicit override val container = new JDBCContainer(
    "yandex/clickhouse-server:latest",
    port -> 8123 :: 9087 -> 9000 :: Nil,
    "ru.yandex.clickhouse.ClickHouseDriver",
    s"jdbc:clickhouse://localhost:$port/default"
  )

  // format: off
  val realWorkloadQuery = """SELECT * FROM (
      |	SELECT toFloat64(number * 10) / 1000.0 as ts, toString(rand() % 2) as t1, toFloat32(rand() % 300) as lt300Sens,       toUInt8(rand() % 10) as lt10Sens,      1000 + (rand() % 5000) as gt1000Sens   FROM numbers(100000)
      |		union all -- 499 1
      |	SELECT toFloat64(number * 10 + 100000) / 1000.0 as ts, toString(1) as t1, 		      toFloat32(1 + (rand() % 299)) as lt300Sens, toUInt8(5 + (rand() % 5)) as lt10Sens, 1000 + (rand() % 5000) as gt1000Sens   FROM numbers(35000)
      |		union all -- 988 1
      |	SELECT toFloat64(number * 10 + 135000) / 1000.0 as ts, toString(1) as t1, 				  toFloat32(0) as lt300Sens,                  toUInt8(1) as lt10Sens,                1000 + (rand() % 5000) as gt1000Sens   FROM numbers(35000)
      |		union all -- 466 1
      |	SELECT toFloat64(number * 10 + 170000) / 1000.0 as ts, toString(1) as t1,          toFloat32(0) as lt300Sens,                  toUInt8(0) as lt10Sens,                5400 + (rand() % 600) as gt1000Sens    FROM numbers(35000)
      |		union all -- 0
      |	SELECT toFloat64(number * 10 + 205000) / 1000.0 as ts, toString(rand() % 2) as t1, toFloat32(rand() % 300) as lt300Sens,       toUInt8(rand() % 10) as lt10Sens,      1000 + (rand() % 5000) as gt1000Sens   FROM numbers(100000)
      |		union all -- 466 2
      |	SELECT toFloat64(number * 10 + 305000) / 1000.0 as ts, toString(2) as t1,          toFloat32(0) as lt300Sens,                  toUInt8(0) as lt10Sens,                5400 + (rand() % 600) as gt1000Sens    FROM numbers(35000)
      |) ORDER BY ts""".stripMargin
  // format: on

  val inputConf = JDBCInputConf(
    sourceId = 123,
    jdbcUrl = container.jdbcUrl,
    query = realWorkloadQuery,
    driverName = container.driverName,
    datetimeField = 'ts,
    eventsMaxGapMs = 60000L,
    defaultEventsGapMs = 200L,
    chunkSizeMs = Some(900000L),
    partitionFields = Seq('t1)
  )

  val sinkSchema =
    RowSchema('series_storage, 'from, 'to, ('app, 1), 'id, 'timestamp, 'context, inputConf.partitionFields)

  val outputConf = JDBCOutputConf(
    "Test.SM_basic_patterns",
    sinkSchema,
    s"jdbc:clickhouse://localhost:$port/default",
    "ru.yandex.clickhouse.ClickHouseDriver"
  )

  val (sumAvgMaxTimeSec, sumAvgPattern) = 50L -> Seq(
    RawPattern("599", "sum(avg(gt1000Sens, 60 sec), 60 sec) > 0")
  )

  override def afterStart(): Unit = {
    super.afterStart()

    Files.readResource("/sql/test-db-schema.sql")
         .mkString
         .split(";")
         .foreach(container.executeUpdate)

    Files.readResource("/sql/wide/sink-schema.sql")
         .mkString
         .split(";")
         .foreach(container.executeUpdate)
  }

  override def afterAll(): Unit = {

    super.afterAll()
    container.stop()

  }

  /*"Aggregators performance tests" should "compute in time" in {

    Post(
      "/streamJob/from-jdbc/to-jdbc/?run_async=0",
      FindPatternsRequest("1", inputConf, outputConf, sumAvgPattern)
    ) ~> route ~> check {

      status shouldEqual StatusCodes.OK
      val resp = unmarshal[FinishedJobResponse](responseEntity)

      resp shouldBe a[Success[_]]
      resp.get.response.execTimeSec should be <= sumAvgMaxTimeSec
    }
  }*/
}
