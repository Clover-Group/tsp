//package ru.itclover.tsp.http
//
//import akka.actor.ActorSystem
//import akka.http.scaladsl.model.StatusCodes
//import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
//import com.dimafeng.testcontainers._
//import com.typesafe.scalalogging.Logger
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//import org.scalatest.FlatSpec
//import ru.itclover.tsp.core.RawPattern
//import ru.itclover.tsp.http.domain.input.FindPatternsRequest
//import ru.itclover.tsp.http.domain.output.SuccessfulResponse.FinishedJobResponse
//import ru.itclover.tsp.http.utils.{JDBCContainer, SqlMatchers}
//import ru.itclover.tsp.io.input.JDBCInputConf
//import ru.itclover.tsp.io.output.{JDBCOutputConf, RowSchema}
//import ru.itclover.tsp.utils.Files
//
//import scala.concurrent.ExecutionContextExecutor
//import scala.concurrent.duration.DurationInt
//import scala.util.Success
//
//class AccumsTest extends FlatSpec with SqlMatchers with ScalatestRouteTest with HttpService with ForAllTestContainer {
//
//  implicit override val executionContext: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global
//  implicit override val streamEnvironment: StreamExecutionEnvironment =
//    StreamExecutionEnvironment.createLocalEnvironment()
//  streamEnvironment.setMaxParallelism(30000) // For proper keyBy partitioning
//
//  private val log = Logger("AccumsTest")
//
//  implicit def defaultTimeout(implicit system: ActorSystem): RouteTestTimeout = RouteTestTimeout(300.seconds)
//
//  val port = 8137
//  implicit override val container: JDBCContainer = new JDBCContainer(
//    "yandex/clickhouse-server:latest",
//    port -> 8123 :: 9089 -> 9000 :: Nil,
//    "ru.yandex.clickhouse.ClickHouseDriver",
//    s"jdbc:clickhouse://localhost:$port/default"
//  )
//
//  val windowLength = 1000
//  val windowMin = 10
//
//  // format: off
//  val realWorkloadQuery: String = """SELECT * FROM (
//      |	SELECT toFloat64(number * 1 + 100000000) as ts, toString(rand() % 2) as t1, toFloat32(rand() % 300) as lt300Sens,       toUInt8(rand() % 10) as lt10Sens,      1000 + (rand() % 5000) as gt1000Sens   FROM numbers(1000)
//      |   union all -- 499 1
//      |	SELECT toFloat64(number * 1 + 100001000) as ts, toString(1) as t1, 		      toFloat32(1 + (rand() % 299)) as lt300Sens, toUInt8(8 + (rand() % 2)) as lt10Sens, 1000 + (rand() % 5000) as gt1000Sens   FROM numbers(1000)
//      |	  union all -- 988 1
//      |	SELECT toFloat64(number * 1 + 100002000) as ts, toString(1) as t1, 				  toFloat32(0) as lt300Sens,                  toUInt8(1) as lt10Sens,                1000 + (rand() % 5000) as gt1000Sens   FROM numbers(1000)
//      |		union all -- 466 1
//      |	SELECT toFloat64(number * 1 + 100003000) as ts, toString(2) as t1,          toFloat32(0) as lt300Sens,                  toUInt8(0) as lt10Sens,                5990 + (rand() % 10) as gt1000Sens    FROM numbers(1000)
//      |		union all -- 0
//      |	SELECT toFloat64(number * 1 + 100004000) as ts, toString(rand() % 2) as t1, toFloat32(rand() % 300) as lt300Sens,       toUInt8(rand() % 10) as lt10Sens,      1000 + (rand() % 5000) as gt1000Sens   FROM numbers(1000)
//      |		union all -- 466 2
//      |	SELECT toFloat64(number * 1 + 100005000) as ts, toString(1) as t1,          toFloat32(0) as lt300Sens,                  toUInt8(0) as lt10Sens,                5990 + (rand() % 10) as gt1000Sens    FROM numbers(1000)
//      |) ORDER BY ts""".stripMargin
//  // format: on
//
//  val (countWindowMaxTimeSec, countWindowPattern) = 150L -> List(
//    RawPattern("4990", s"lt10Sens >= 8 for $windowMin min >= ${windowMin * 60 - 30} times")
//  )
//
//  val (timeWindowMaxTimeSec, timeWindowPattern) = 250L -> List(
//    RawPattern("499", s"lt10Sens >= 8 for $windowMin min > ${windowMin - 1} min")
//  )
//
//  val (nestedTimeWindowMaxTimeSec, nestedTimeWindowPattern) = 175L -> List(
//    RawPattern("4991", s"(avg(lt10Sens as float64, 30 sec) >= 8.0) for $windowMin min > ${windowMin - 1} min")
//  )
//
//  val (timeWindowCountMaxTimeSec, timeWindowCountPattern) = 60L -> List(
//    RawPattern("988", s"lt10Sens = 1 for $windowMin min > ${windowMin * 60 - 1} times")
//  )
//
//  val (timedMaxTimeSec, timedPattern) = 75L -> List(
//    RawPattern("466", s"gt1000Sens >= 5990 for $windowMin min")
//  )
//
//  val (reducerMaxTimeSec, reducerPattern) = 100L -> List(
//    RawPattern("467", "avgOf(1.0, 0.0) < 200")
//  )
//
//  val inputConf = JDBCInputConf(
//    sourceId = 123,
//    jdbcUrl = container.jdbcUrl,
//    query = realWorkloadQuery,
//    driverName = container.driverName,
//    datetimeField = 'ts,
//    eventsMaxGapMs = 2000L,
//    defaultEventsGapMs = 2000L,
//    chunkSizeMs = Some(900000L),
//    partitionFields = Seq('t1)
//  )
//
//  val sinkSchema =
//    RowSchema('series_storage, 'from, 'to, ('app, 1), 'id, 'timestamp, 'context, inputConf.partitionFields)
//
//  val outputConf = JDBCOutputConf(
//    "Test.SM_basic_patterns",
//    sinkSchema,
//    s"jdbc:clickhouse://localhost:$port/default",
//    "ru.yandex.clickhouse.ClickHouseDriver"
//  )
//
//  override def afterStart(): Unit = {
//    super.beforeAll()
//    Files.readResource("/sql/test-db-schema.sql").mkString.split(";").map(container.executeUpdate)
//    Files.readResource("/sql/wide/sink-schema.sql").mkString.split(";").map(container.executeUpdate)
//  }
//
//  "Count window (count)" should "compute in time" in {
//    Post(
//      "/streamJob/from-jdbc/to-jdbc/?run_async=0",
//      FindPatternsRequest("1", inputConf, outputConf, countWindowPattern)
//    ) ~> route ~> check {
//
//      status shouldEqual StatusCodes.OK
//      val resp = unmarshal[FinishedJobResponse](responseEntity)
//      resp shouldBe a[Success[_]]
//      val execTimeS = resp.get.response.execTimeSec
//      log.info(s"Test job completed for $execTimeS sec.")
//
//      // Correctness
//      checkByQuery(
//        1 :: Nil,
//        "SELECT count(*) FROM Test.SM_basic_patterns WHERE id = 4990 AND to - from > 900"
//      )
//      // Performance
//      execTimeS should be <= countWindowMaxTimeSec
//    }
//  }
//
//  "Time window (truthMillis)" should "compute in time" in {
//    Post("/streamJob/from-jdbc/to-jdbc/?run_async=0", FindPatternsRequest("1", inputConf, outputConf, timeWindowPattern)) ~> route ~> check {
//
//      status shouldEqual StatusCodes.OK
//      val resp = unmarshal[FinishedJobResponse](responseEntity)
//      resp shouldBe a[Success[_]]
//      val execTimeS = resp.get.response.execTimeSec
//      log.info(s"Test job completed for $execTimeS sec.")
//
//      // Correctness
//      checkByQuery(
//        1 :: Nil,
//        "SELECT count(*) FROM Test.SM_basic_patterns WHERE id = 499 AND to - from > 990"
//      )
//      // Performance
//      execTimeS should be <= timeWindowMaxTimeSec
//    }
//  }
//
//  "Nested time window (truthMillis)" should "compute in time" in {
//    Post(
//      "/streamJob/from-jdbc/to-jdbc/?run_async=0",
//      FindPatternsRequest("1", inputConf, outputConf, nestedTimeWindowPattern)
//    ) ~> route ~> check {
//
//      status shouldEqual StatusCodes.OK
//      val resp = unmarshal[FinishedJobResponse](responseEntity)
//      resp shouldBe a[Success[_]]
//      val execTimeS = resp.get.response.execTimeSec
//      log.info(s"Test job completed for $execTimeS sec.")
//
//      // Correctness
//      checkByQuery(
//        1 :: Nil,
//        "SELECT count(*) FROM Test.SM_basic_patterns WHERE id = 4991 AND to - from > 990"
//      )
//      // Performance
//      execTimeS should be <= nestedTimeWindowMaxTimeSec
//    }
//  }
//
//  "Time window count (truthMillisCount)" should "compute in time" in {
//    Post(
//      "/streamJob/from-jdbc/to-jdbc/?run_async=0",
//      FindPatternsRequest("2", inputConf, outputConf, timeWindowCountPattern)
//    ) ~> route ~> check {
//
//      status shouldEqual StatusCodes.OK
//      val resp = unmarshal[FinishedJobResponse](responseEntity)
//      resp shouldBe a[Success[_]]
//      val execTimeS = resp.get.response.execTimeSec
//      log.info(s"Test job completed for $execTimeS sec.")
//
//      // Correctness
//      checkByQuery(
//        1 :: Nil,
//        "SELECT count(*) FROM Test.SM_basic_patterns WHERE id = 988 AND to - from > 990"
//      )
//      // Performance
//      execTimeS should be <= timeWindowCountMaxTimeSec
//    }
//  }
//
//  "Timed window (.timed)" should "compute in time" in {
//    Post("/streamJob/from-jdbc/to-jdbc/?run_async=0", FindPatternsRequest("3", inputConf, outputConf, timedPattern)) ~> route ~> check {
//
//      status shouldEqual StatusCodes.OK
//      val resp = unmarshal[FinishedJobResponse](responseEntity)
//      resp shouldBe a[Success[_]]
//      val execTimeS = resp.get.response.execTimeSec
//      log.info(s"Test job completed for $execTimeS sec.")
//
//      // Correctness
//      checkByQuery(
//        2 :: Nil,
//        "SELECT count(*) FROM Test.SM_basic_patterns WHERE id = 466 AND to - from > 990"
//      )
//      // Performance
//      execTimeS should be <= timedMaxTimeSec
//    }
//
//  }
//
//  "Reducer (.avgOf)" should "compute in time" in {
//    Post("/streamJob/from-jdbc/to-jdbc/?run_async=0", FindPatternsRequest("3", inputConf, outputConf, reducerPattern)) ~> route ~> check {
//
//      status shouldEqual StatusCodes.OK
//      val resp = unmarshal[FinishedJobResponse](responseEntity)
//      resp shouldBe a[Success[_]]
//      val execTimeS = resp.get.response.execTimeSec
//      log.info(s"Test job completed for $execTimeS sec.")
//
//      // Correctness
//      checkByQuery(
//        1 :: Nil,
//        "SELECT count(*) FROM Test.SM_basic_patterns WHERE id = 467"
//      )
//      // Performance
//      execTimeS should be <= reducerMaxTimeSec
//    }
//
//  }
//}
