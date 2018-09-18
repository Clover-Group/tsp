package ru.itclover.tsp.http

import com.dimafeng.testcontainers._
import com.typesafe.scalalogging.Logger
import org.scalatest.FlatSpec
import ru.itclover.tsp.http.domain.input.FindPatternsRequest
import ru.itclover.tsp.http.utils.{JDBCContainer, HttpServiceMathers}
import ru.itclover.tsp.io.input.{JDBCInputConf, RawPattern}
import ru.itclover.tsp.io.output.{JDBCOutputConf, RowSchema}
import ru.itclover.tsp.utils.Files

class AccumsPerfTest extends FlatSpec with HttpServiceMathers with ForAllTestContainer {

  override val log = Logger("AccumsPerfTest")

  val port = 8137
  implicit override val container = new JDBCContainer(
    "yandex/clickhouse-server:latest",
    port -> 8123 :: 9089 -> 9000 :: Nil,
    "ru.yandex.clickhouse.ClickHouseDriver",
    s"jdbc:clickhouse://localhost:$port/default"
  )

  //
  // format: off
  val realWorkloadQuery = """
    SELECT * FROM (
    |	SELECT toFloat64(number * 1 + 100000000) as ts, toString(rand() % 2) as t1, toFloat32(rand() % 300) as lt300Sens,       toUInt8(rand() % 10) as lt10Sens,      1000 + (rand() % 5000) as gt1000Sens   FROM numbers(100000)
    |		union all -- 499 1
    |	SELECT toFloat64(number * 1 + 100100000) as ts, toString(1) as t1, 		      toFloat32(1 + (rand() % 299)) as lt300Sens, toUInt8(8 + (rand() % 2)) as lt10Sens, 1000 + (rand() % 5000) as gt1000Sens   FROM numbers(100000)
    |		union all -- 988 1
    |	SELECT toFloat64(number * 1 + 100200000) as ts, toString(1) as t1, 				  toFloat32(0) as lt300Sens,                  toUInt8(1) as lt10Sens,                1000 + (rand() % 5000) as gt1000Sens   FROM numbers(100000)
    |		union all -- 466 1
    |	SELECT toFloat64(number * 1 + 100300000) as ts, toString(2) as t1,          toFloat32(0) as lt300Sens,                  toUInt8(0) as lt10Sens,                5990 + (rand() % 10) as gt1000Sens    FROM numbers(100000)
    |		union all -- 0
    |	SELECT toFloat64(number * 1 + 100400000) as ts, toString(rand() % 2) as t1, toFloat32(rand() % 300) as lt300Sens,       toUInt8(rand() % 10) as lt10Sens,      1000 + (rand() % 5000) as gt1000Sens   FROM numbers(100000)
    |		union all -- 466 2
    |	SELECT toFloat64(number * 1 + 100500000) as ts, toString(1) as t1,          toFloat32(0) as lt300Sens,                  toUInt8(0) as lt10Sens,                5990 + (rand() % 10) as gt1000Sens    FROM numbers(100000)
    |) ORDER BY ts""".stripMargin
  // format: on

  val inputConf = JDBCInputConf(
    sourceId = 123,
    jdbcUrl = container.jdbcUrl,
    query = realWorkloadQuery,
    driverName = container.driverName,
    datetimeField = 'ts,
    eventsMaxGapMs = 2000L,
    defaultEventsGapMs = 2000L,
    partitionFields = Seq('t1)
  )

  val sinkSchema =
    RowSchema('series_storage, 'from, 'to, ('app, 1), 'id, 'timestamp, 'context, inputConf.partitionFields)

  val outputConf = JDBCOutputConf(
    "Test.SM_basic_wide_patterns",
    sinkSchema,
    s"jdbc:clickhouse://localhost:$port/default",
    "ru.yandex.clickhouse.ClickHouseDriver"
  )

  val windowMin = 20

  val (timeWindowMaxTimeSec, timeWindowPattern) = 250L -> List(
    RawPattern("499", s"lt10Sens >= 8 for $windowMin min > ${windowMin - 1} min")
  )

  val (nestedTimeWindowMaxTimeSec, nestedTimeWindowPattern) = 175L -> List(
    RawPattern("4991", s"(avg(lt10Sens, 30 sec) >= 8) for $windowMin min > ${windowMin - 1} min")
  )

  val (timeWindowCountMaxTimeSec, timeWindowCountPattern) = 60L -> List(
    RawPattern("988", s"lt10Sens = 1 for $windowMin min > ${windowMin * 60 - 1} times")
  )

  val (timedMaxTimeSec, timedPattern) = 75L -> List(
    RawPattern("466", s"gt1000Sens >= 5990 for $windowMin min")
  )

  override def afterStart(): Unit = {
    super.beforeAll()
    Files.readResource("/sql/test-db-schema.sql").mkString.split(";").map(container.executeUpdate)
    Files.readResource("/sql/wide/sink-schema.sql").mkString.split(";").map(container.executeUpdate)
  }

  // .. Increase windows in rules and numbers, check sbt task, make start script
  "Time window (truthMillis)" should "compute in time" in {
    Post("/streamJob/from-jdbc/to-jdbc/?run_async=0", FindPatternsRequest("1", inputConf, outputConf, timeWindowPattern)) ~> route ~> check {
      val execTimeS = checkAndGetExecTimeSec()
      // Correctness
      checkByQuery(
        1 :: Nil,
        "SELECT count(*) FROM Test.SM_basic_wide_patterns WHERE id = 499 AND to - from > 99000"
      )
      // Performance
      execTimeS should be <= timeWindowMaxTimeSec
    }
  }

  "Nested time window (truthMillis)" should "compute in time" in {
    Post(
      "/streamJob/from-jdbc/to-jdbc/?run_async=0",
      FindPatternsRequest("1", inputConf, outputConf, nestedTimeWindowPattern)
    ) ~> route ~> check {
      val execTimeS = checkAndGetExecTimeSec()
      // Correctness
      checkByQuery(
        1 :: Nil,
        "SELECT count(*) FROM Test.SM_basic_wide_patterns WHERE id = 4991 AND to - from > 99000"
      )
      // Performance
      execTimeS should be <= nestedTimeWindowMaxTimeSec
    }
  }

  "Time window count (truthMillisCount)" should "compute in time" in {
    Post(
      "/streamJob/from-jdbc/to-jdbc/?run_async=0",
      FindPatternsRequest("2", inputConf, outputConf, timeWindowCountPattern)
    ) ~> route ~> check {
      val execTimeS = checkAndGetExecTimeSec()
      // Correctness
      checkByQuery(
        1 :: Nil,
        "SELECT count(*) FROM Test.SM_basic_wide_patterns WHERE id = 988 AND to - from > 99000"
      )
      // Performance
      execTimeS should be <= timeWindowCountMaxTimeSec
    }
  }

  "Timed window (.timed)" should "compute in time" in {
    Post("/streamJob/from-jdbc/to-jdbc/?run_async=0", FindPatternsRequest("3", inputConf, outputConf, timedPattern)) ~> route ~> check {
      val execTimeS = checkAndGetExecTimeSec()
      // Correctness
      checkByQuery(
        2 :: Nil,
        "SELECT count(*) FROM Test.SM_basic_wide_patterns WHERE id = 466 AND to - from > 99000"
      )
      // Performance
      execTimeS should be <= timedMaxTimeSec
    }
  }
}
