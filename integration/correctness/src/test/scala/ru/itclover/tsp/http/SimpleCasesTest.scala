package ru.itclover.tsp.http

import java.util.concurrent.{SynchronousQueue, ThreadPoolExecutor, TimeUnit}

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import com.dimafeng.testcontainers._
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.scalatest.FlatSpec
import org.testcontainers.containers.wait.strategy.Wait
import ru.itclover.tsp.RowWithIdx
import ru.itclover.tsp.core.RawPattern
import ru.itclover.tsp.http.domain.input.FindPatternsRequest
import ru.itclover.tsp.http.protocols.RoutesProtocols
import ru.itclover.tsp.http.utils.{JDBCContainer, SqlMatchers}
import ru.itclover.tsp.io.input.{JDBCInputConf, NarrowDataUnfolding}
import ru.itclover.tsp.io.output.{JDBCOutputConf, RowSchema}
import ru.itclover.tsp.utils.Files

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

class SimpleCasesTest extends FlatSpec with SqlMatchers with ScalatestRouteTest with HttpService with ForAllTestContainer with RoutesProtocols {
  implicit override val executionContext: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global
  implicit override val streamEnvironment: StreamExecutionEnvironment =
    StreamExecutionEnvironment.createLocalEnvironment()
  streamEnvironment.setMaxParallelism(30000) // For proper keyBy partitioning

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

  val port = 8161
  implicit override val container = new JDBCContainer(
    "yandex/clickhouse-server:latest",
    port -> 8123 :: 9098 -> 9000 :: Nil,
    "ru.yandex.clickhouse.ClickHouseDriver",
    s"jdbc:clickhouse://localhost:$port/default",
    waitStrategy = Some(Wait.forHttp("/"))
  )

  val casesPatterns = Seq(
    RawPattern("1", "PowerPolling = 50"),
    RawPattern("2", "PowerPolling > 20"),
    RawPattern("3", "PowerPolling >= 20"),
    RawPattern("4", "PowerPolling < 20"),
    RawPattern("5", "PowerPolling <= 20"),
    RawPattern("6", "PowerPolling <> 70"),
    RawPattern("7", "POilDieselOut > 9.52 and SpeedThrustMin = 51"),
    RawPattern("8", "POilDieselOut >= 9.52 for 2 sec"),
    RawPattern("9", "POilDieselOut <= 9.53 and SpeedThrustMin =51 andThen SpeedThrustMin = 52"),
    RawPattern("10", "POilDieselOut = 9.53 or SpeedThrustMin = 51"),
    RawPattern("11", "POilDieselOut < 9.50 until SpeedThrustMin > 51"),
    RawPattern("12", "abs(SpeedThrustMin + POilDieselOut) > 40"),
    RawPattern("13", "avg(SpeedThrustMin, 2 sec) = 22"),
    RawPattern("14", "avgOf(POilDieselOut, SpeedThrustMin) > 0"),
    RawPattern("15", "lag(POilDieselOut) < 0"),
    RawPattern("16", "skip(1 sec, SpeedThrustMin = 0 for 1 sec andThen SpeedThrustMin > 40)"),
    RawPattern("17", "POilDieselOut = 9.53 andThen Skip(3 sec, POilDieselOut < 9.50 for 3 sec)")
  )

  val incidentsCount = Map(
    1 -> 9,
    2 -> 5,
    3 -> 3,
    4 -> 1,
    5 -> 6,
    6 -> 6,
    7 -> 1,
    8 -> 1,
    9 -> 1,
    10 -> 1,
    11 -> 3,
    12 -> 2,
    13 -> 1,
    14 -> 3,
    15 -> 1,
    16 -> 1,
    17 -> 1,
  )

  val wideInputConf = JDBCInputConf(
    sourceId = 100,
    jdbcUrl = container.jdbcUrl,
    query =  "SELECT * FROM `2te116u_tmy_test_simple_rules` ORDER BY ts",
    driverName = container.driverName,
    datetimeField = 'ts,
    eventsMaxGapMs = 60000L,
    defaultEventsGapMs = 1000L,
    chunkSizeMs = Some(900000L),
    partitionFields = Seq('loco_num, 'section, 'upload_id)
  )

  val narrowInputConf = JDBCInputConf(
    sourceId = 200,
    jdbcUrl = container.jdbcUrl,
    query =  "SELECT * FROM math_test ORDER BY dt",
    driverName = container.driverName,
    datetimeField = 'dt,
    eventsMaxGapMs = 60000L,
    defaultEventsGapMs = 1000L,
    chunkSizeMs = Some(900000L),
    partitionFields = Seq('loco_num, 'section, 'upload_id),
    dataTransformation = Some(NarrowDataUnfolding('sensor_id, 'value_float, Map(), Some(1000))),
  )

  val wideRowSchema = RowSchema('series_storage, 'from, 'to, ('app, 1), 'id, 'timestamp, 'context, wideInputConf.partitionFields)
  val narrowRowSchema = RowSchema('series_storage, 'from, 'to, ('app, 2), 'id, 'timestamp, 'context, narrowInputConf.partitionFields)

  val wideOutputConf = JDBCOutputConf(
    "events_wide_test",
    wideRowSchema,
    s"jdbc:clickhouse://localhost:$port/default",
    "ru.yandex.clickhouse.ClickHouseDriver"
  )

  val narrowOutputConf = JDBCOutputConf(
    "events_narrow_test",
    narrowRowSchema,
    s"jdbc:clickhouse://localhost:$port/default",
    "ru.yandex.clickhouse.ClickHouseDriver"
  )


  override def afterStart(): Unit = {
    super.afterStart()
    Files.readResource("/sql/test/cases-narrow-schema-new.sql").mkString.split(";").foreach(container.executeUpdate)
    Files.readResource("/sql/test/cases-wide-schema-new.sql").mkString.split(";").foreach(container.executeUpdate)
    container.executeUpdate(s"INSERT INTO math_test FORMAT CSV\n${Files.readResource("/sql/test/cases-narrow-new.csv").drop(1).mkString("\n")}")
    container.executeUpdate(s"INSERT INTO `2te116u_tmy_test_simple_rules` FORMAT CSV\n${Files.readResource("/sql/test/cases-wide-new.csv").drop(1).mkString("\n")}")
    Files.readResource("/sql/test/cases-sinks-schema.sql").mkString.split(";").foreach(container.executeUpdate)

  }

  "Data" should "load properly" in {
    checkByQuery(List(List(27.0)), "SELECT COUNT(*) FROM `2te116u_tmy_test_simple_rules`")
    checkByQuery(List(List(81.0)), "SELECT COUNT(*) FROM math_test")
  }

  "Cases 1-17" should "work in wide table" in {
    (1 to 17).foreach { id =>
      Post("/streamJob/from-jdbc/to-jdbc/?run_async=0",
        FindPatternsRequest(s"17wide_$id", wideInputConf, wideOutputConf, List(casesPatterns(id - 1)))) ~>
        route ~> check {
        withClue(s"Pattern ID: $id") {
          status shouldEqual StatusCodes.OK
        }
          //checkByQuery(List(List(id.toDouble, incidentsCount(id).toDouble)), s"SELECT $id, COUNT(*) FROM events_wide_test WHERE id = $id")
      }
    }
    checkByQuery(incidentsCount.map {
      case (k, v) => List(k.toDouble, v.toDouble)
    }.toList.sortBy(_.head), s"SELECT id, COUNT(*) FROM events_wide_test GROUP BY id ORDER BY id")
  }

  "Cases 1-17" should "work in narrow table" in {
    (1 to 17).foreach { id =>
      Post("/streamJob/from-jdbc/to-jdbc/?run_async=0",
        FindPatternsRequest(s"17narrow_$id", narrowInputConf, narrowOutputConf, List(casesPatterns(id - 1)))) ~>
        route ~> check {
        withClue(s"Pattern ID: $id") {
          status shouldEqual StatusCodes.OK
        }
        //checkByQuery(List(List(id.toDouble, incidentsCount(id).toDouble)), s"SELECT $id, COUNT(*) FROM events_narrow_test WHERE id = $id")
      }
    }
    checkByQuery(incidentsCount.map {
      case (k, v) => List(k.toDouble, v.toDouble)
    }.toList.sortBy(_.head), s"SELECT id, COUNT(*) FROM events_narrow_test GROUP BY id ORDER BY id")
  }
}
