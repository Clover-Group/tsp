package ru.itclover.tsp.http

import java.util.concurrent.{SynchronousQueue, ThreadPoolExecutor, TimeUnit}
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import com.dimafeng.testcontainers.ForAllTestContainer
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.spark.sql.SparkSession
import org.scalatest.FlatSpec
import org.testcontainers.containers.wait.strategy.Wait
import ru.itclover.tsp.RowWithIdx
import ru.itclover.tsp.core.RawPattern
import ru.itclover.tsp.http.domain.input.FindPatternsRequest
import ru.itclover.tsp.http.utils.{JDBCContainer, SqlMatchers}
import ru.itclover.tsp.io.input.{JDBCInputConf, NarrowDataUnfolding}
import ru.itclover.tsp.io.output.{JDBCOutputConf, NewRowSchema}
import ru.itclover.tsp.utils.Files

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

// In test cases, 'should' expressions are non-unit. Suppressing wartremover warnings about it
// Also, some test cases indirectly use Any type.
@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements", "org.wartremover.warts.Any"))
class NarrowTableTest
    extends FlatSpec
    with SqlMatchers
    with ScalatestRouteTest
    with HttpService
    with ForAllTestContainer {

  implicit override val executionContext: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global
  implicit override val streamEnvironment: StreamExecutionEnvironment =
    StreamExecutionEnvironment.createLocalEnvironment()
  streamEnvironment.setMaxParallelism(30000) // For proper keyBy partitioning

  val spark = SparkSession
    .builder()
    .master("local")
    .appName("TSP Spark test")
    .config("spark.io.compression.codec", "snappy")
    .getOrCreate()

  // to run blocking tasks.
  val blockingExecutorContext: ExecutionContextExecutor =
    ExecutionContext.fromExecutor(
      new ThreadPoolExecutor(
        0, // corePoolSize
        Int.MaxValue, // maxPoolSize
        1000L, //keepAliveTime
        TimeUnit.MILLISECONDS, //timeUnit
        new SynchronousQueue[Runnable](), //workQueue
        new ThreadFactoryBuilder().setNameFormat("blocking-thread").setDaemon(true).build()
      )
    )

  implicit def defaultTimeout = RouteTestTimeout(300.seconds)

  val port = 8151
  implicit override val container = new JDBCContainer(
    "yandex/clickhouse-server:latest",
    port -> 8123 :: 9088 -> 9000 :: Nil,
    "ru.yandex.clickhouse.ClickHouseDriver",
    s"jdbc:clickhouse://localhost:$port/default",
    // 400 for the native CH port
    waitStrategy = Some(Wait.forHttp("/").forStatusCode(200).forStatusCode(400))
  )

  val transformation = NarrowDataUnfolding[RowWithIdx, Symbol, Any](
    'key,
    'value,
    Map('speed1 -> 1000, 'speed2 -> 1000)
  )

  val inputConf = JDBCInputConf(
    sourceId = 123,
    jdbcUrl = container.jdbcUrl,
    query = """select * from Test.SM_basic_narrow""", // speed(1)(2) fancy colnames test
    driverName = container.driverName,
    datetimeField = 'datetime,
    eventsMaxGapMs = Some(60000L),
    defaultEventsGapMs = Some(1000L),
    chunkSizeMs = Some(900000L),
    partitionFields = Seq('series_id, 'mechanism_id),
    dataTransformation = Some(transformation)
  )

  val rowSchema = NewRowSchema('series_storage, 'from, 'to, ('app, 1), 'id, 'subunit)

  val outputConf = JDBCOutputConf(
    "Test.SM_basic_patterns",
    rowSchema,
    s"jdbc:clickhouse://localhost:$port/default",
    "ru.yandex.clickhouse.ClickHouseDriver"
  )

  val basicAssertions = Seq(
    RawPattern(1, "speed1 < 15"),
    RawPattern(2, """"speed2" > 10""")
  )

  override def afterStart(): Unit = {
    super.afterStart()

    Files
      .readResource("/sql/test-db-schema.sql")
      .mkString
      .split(";")
      .foreach(container.executeUpdate)

    Files
      .readResource("/sql/narrow/source-schema.sql")
      .mkString
      .split(";")
      .foreach(container.executeUpdate)

    Files
      .readResource("/sql/narrow/source-inserts.sql")
      .mkString
      .split(";")
      .foreach(container.executeUpdate)

    Files
      .readResource("/sql/sink-schema.sql")
      .mkString
      .split(";")
      .foreach(container.executeUpdate)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    container.stop()
  }

  "Basic assertions and forwarded fields" should "work for wide dense table" in {
    Post("/streamJob/from-jdbc/to-jdbc/?run_async=0", FindPatternsRequest("1", inputConf, outputConf, basicAssertions)) ~>
    route ~> check {
      //status shouldEqual StatusCodes.OK
    }
  }
}
