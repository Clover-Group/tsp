//package ru.itclover.tsp.http
//
//import java.util.concurrent.{SynchronousQueue, ThreadPoolExecutor, TimeUnit}
//
//import akka.http.scaladsl.model.StatusCodes
//import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
//import com.dimafeng.testcontainers._
//import com.google.common.util.concurrent.ThreadFactoryBuilder
//import com.typesafe.scalalogging.Logger
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//import org.scalatest.FlatSpec
//import org.testcontainers.containers.wait.strategy.Wait
//import ru.itclover.tsp.core.RawPattern
//import ru.itclover.tsp.http.domain.input.FindPatternsRequest
//import ru.itclover.tsp.http.domain.output.SuccessfulResponse.FinishedJobResponse
//import ru.itclover.tsp.http.utils.{JDBCContainer, SqlMatchers}
//import ru.itclover.tsp.io.input.JDBCInputConf
//import ru.itclover.tsp.io.output.{JDBCOutputConf, RowSchema}
//import ru.itclover.tsp.utils.Files
//
//import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
//import scala.concurrent.duration.DurationInt
//import scala.util.Success
//
//class RealDataTest extends FlatSpec with SqlMatchers with ScalatestRouteTest with HttpService with ForAllTestContainer {
//
//  implicit def defaultTimeout = RouteTestTimeout(300.seconds)
//  implicit override val executionContext: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global
//  implicit override val streamEnvironment: StreamExecutionEnvironment =
//    StreamExecutionEnvironment.createLocalEnvironment()
//  streamEnvironment.setMaxParallelism(30000) // For proper keyBy partitioning
//
//  // to run blocking tasks.
//  val blockingExecutorContext: ExecutionContextExecutor =
//    ExecutionContext.fromExecutor(
//      new ThreadPoolExecutor(
//        0, // corePoolSize
//        Int.MaxValue, // maxPoolSize
//        1000L, //keepAliveTime
//        TimeUnit.MILLISECONDS, //timeUnit
//        new SynchronousQueue[Runnable](), //workQueue
//        new ThreadFactoryBuilder().setNameFormat("blocking-thread").setDaemon(true).build()
//      )
//    )
//
//  private val log = Logger("RealDataTest")
//
//  val port = 8136
//
//  implicit override val container = new JDBCContainer(
//    "yandex/clickhouse-server:latest",
//    port -> 8123 :: 9083 -> 9000 :: Nil,
//    "ru.yandex.clickhouse.ClickHouseDriver",
//    s"jdbc:clickhouse://localhost:$port/default",
//    waitStrategy = Some(Wait.forHttp("/"))
//  )
//
//  val inputConf = JDBCInputConf(
//    sourceId = 123,
//    jdbcUrl = container.jdbcUrl,
//    query = "select * from Test.Bigdata_HI",
//    driverName = container.driverName,
//    datetimeField = 'dt,
//    eventsMaxGapMs = 60000L,
//    defaultEventsGapMs = 10000L,
//    chunkSizeMs = Some(900000L),
//    partitionFields = Seq('stock_num),
//    patternsParallelism = Some(1)
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
//  val (timeRangeSec, assertions) = (1 to 80) -> Seq(
//      RawPattern("6", "HI__wagon_id__6 < 0.5"),
//      RawPattern("4", "HI__wagon_id__4 < 0.5")
//    )
//
//  override def afterStart(): Unit = {
//    super.beforeAll()
//    Files.readResource("/sql/test-db-schema.sql").mkString.split(";").foreach(container.executeUpdate)
//    Files.readResource("/sql/wide/source_bigdata_HI_115k.sql").mkString.split(";").foreach(container.executeUpdate)
//    Files.readResource("/sql/sink-schema.sql").mkString.split(";").foreach(container.executeUpdate)
//  }
//
//  "Basic assertions" should "work for wide dense table" in {
//
//    Post("/streamJob/from-jdbc/to-jdbc/?run_async=0", FindPatternsRequest("1", inputConf, outputConf, assertions)) ~>
//    route ~> check {
//      status shouldEqual StatusCodes.OK
//      val resp = unmarshal[FinishedJobResponse](responseEntity)
//      resp shouldBe a[Success[_]]
//      val execTimeS = resp.get.response.execTimeSec
//      log.info(s"Test job completed for $execTimeS sec.")
//
//      // Correctness TODO: check the actual values
//      checkByQuery(686.0 :: Nil, "SELECT count(*) FROM Test.SM_basic_patterns WHERE id = 6") // was 1275
//      checkByQuery(1078.0 :: Nil, "SELECT count(*) FROM Test.SM_basic_patterns WHERE id = 4") // was 1832
//
//      // Performance
//      val fromT = timeRangeSec.head.toLong
//      val toT = timeRangeSec.last.toLong
//      execTimeS should ((be >= fromT).and(be <= toT))
//    }
//  }
//}
