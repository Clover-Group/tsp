package ru.itclover.tsp.http

import java.util.concurrent.{SynchronousQueue, ThreadPoolExecutor, TimeUnit}

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import com.dimafeng.testcontainers._
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.scalatest.FlatSpec
import org.testcontainers.containers.wait.strategy.Wait
import ru.itclover.tsp.core.RawPattern
import ru.itclover.tsp.http.domain.input.FindPatternsRequest
import ru.itclover.tsp.http.protocols.RoutesProtocols
import ru.itclover.tsp.http.utils.{InfluxDBContainer, JDBCContainer, SqlMatchers}
import ru.itclover.tsp.io.input.{InfluxDBInputConf, JDBCInputConf, NarrowDataUnfolding, WideDataFilling}
import ru.itclover.tsp.io.output.{JDBCOutputConf, RowSchema}
import ru.itclover.tsp.utils.Files
import spray.json._

import scala.util.{Try,Success}
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

class SimpleCasesTest
    extends FlatSpec
    with SqlMatchers
    with ScalatestRouteTest
    with HttpService
    with ForAllTestContainer
    with RoutesProtocols {
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
  val influxPort = 8144
  implicit val clickhouseContainer = new JDBCContainer(
    "yandex/clickhouse-server:latest",
    port -> 8123 :: 9098 -> 9000 :: Nil,
    "ru.yandex.clickhouse.ClickHouseDriver",
    s"jdbc:clickhouse://localhost:$port/default",
    waitStrategy = Some(Wait.forHttp("/"))
  )

  implicit val influxContainer =
    new InfluxDBContainer(
      "influxdb:1.7",
      influxPort -> 8086 :: Nil,
      s"http://localhost:$influxPort",
      "Test",
      "default",
      waitStrategy = Some(Wait.forHttp("/").forStatusCode(200).forStatusCode(404))
    )

  override val container = MultipleContainers(LazyContainer(clickhouseContainer), LazyContainer(influxContainer))

  var sourcePatternsString = ""

  val fileResult: Try[String] = Files.readFile("integration/correctness/src/test/resources/simple_cases/patterns.json")

  fileResult match {
    case Success(some) => {
      sourcePatternsString = some
    }
  }

  val jsonObject = sourcePatternsString.parseJson
  val casesPatterns = jsonObject.convertTo[Seq[RawPattern]]

  val incidentsCount = Map(
    1  -> 9,
    2  -> 5,
    3  -> 3,
    4  -> 1,
    5  -> 6,
    6  -> 6,
    7  -> 1,
    8  -> 1,
    9  -> 1,
    10 -> 1,
    11 -> 2, // TODO: or 3?
    12 -> 2,
    13 -> 1,
    14 -> 3,
    15 -> 1,
    16 -> 1,
    17 -> 1,
  )
  val incidentsIvolgaCount = Map(
    18 -> 1,
    19 -> 2,
    20 -> 1,
    21 -> 2,
    22 -> 2,
    23 -> 1,
    24 -> 1,
    25 -> 1,
    26 -> 1,
    27 -> 1,
    28 -> 2,
    29 -> 1,
    30 -> 2,
    31 -> 2,
    32 -> 1,
    33 -> 1,
    34 -> 1,
    35 -> 1,
    36 -> 0,
    37 -> 0,
    38 -> 1,
    39 -> 1,
    40 -> 1,
    41 -> 1,
    42 -> 1,
  )

  // type is explicitly specified to avoid writing pattern ID as Double
  val incidentsTimestamps: List[List[Double]] = List(
    List(1, 1549561527.0, 1549561527.0),
    List(1, 1549561529.0, 1549561529.0),
    List(1, 1552176056.0, 1552176056.0),
    List(1, 1552176058.0, 1552176058.0),
    List(1, 1552176061.0, 1552176061.0),
    List(1, 1552860730.0, 1552860730.0),
    List(1, 1552860728.0, 1552860728.0),
    List(1, 1552860733.0, 1552860733.0),
    List(1, 1552860735.0, 1552860735.0),
    List(2, 1549561527.0, 1549561530.0),
    List(2, 1552176056.0, 1552176059.0),
    List(2, 1552176061.0, 1552176062.0),
    List(2, 1552860728.0, 1552860731.0),
    List(2, 1552860733.0, 1552860738.0),
    List(3, 1549561526.0, 1549561530.0),
    List(3, 1552176055.0, 1552176062.0),
    List(3, 1552860727.0, 1552860738.0),
    List(4, 1549561531.0, 1549561532.0),
    List(5, 1549561526.0, 1549561526.0),
    List(5, 1549561531.0, 1549561532.0),
    List(5, 1552176055.0, 1552176055.0),
    List(5, 1552176060.0, 1552176060.0),
    List(5, 1552860727.0, 1552860727.0),
    List(5, 1552860732.0, 1552860732.0),
    List(6, 1549561526.0, 1549561529.0),
    List(6, 1549561531.0, 1549561532.0),
    List(6, 1552176055.0, 1552176058.0),
    List(6, 1552176060.0, 1552176062.0),
    List(6, 1552860727.0, 1552860730.0),
    List(6, 1552860732.0, 1552860735.0),
    List(7, 1552860727.0, 1552860731.0),
    List(8, 1552860727.0, 1552860731.0),
    List(9, 1552860727.0, 1552860738.0),
    List(10, 1552860727.0, 1552860734.0),
    List(11, 1549561526.0, 1549561532.0), // until the end of chunk
    List(11, 1552176055.0, 1552176062.0), // until the end of chunk
    // List(11, 1552860727.0, 1552860734.0), // TODO: must fire here, needs investigation
    List(12, 1552176058.0, 1552176062.0),
    List(12, 1552860727.0, 1552860738.0),
    List(13, 1552176058.0, 1552176058.0),
    List(14, 1549561526.0, 1549561532.0),
    List(14, 1552176058.0, 1552176062.0),
    List(14, 1552860727.0, 1552860738.0),
    List(15, 1552176056.0, 1552176059.0),
    List(16, 1552176055.0, 1552176062.0),
    List(17, 1552860727.0, 1552860736.0),
  )

  val incidentsIvolgaTimestamps: List[List[Double]] = List(
    List(18, 1572120331.0, 1572120331.0),
    List(19, 1572120320.0, 1572120343.0),
    List(19, 1572120345.0, 1572120367.0),
    List(20, 1572120321.0, 1572120344.0),
    List(21, 1572120320.0, 1572120320.0),
    List(21, 1572120339.0, 1572120339.0),
    List(22, 1572120332.0, 1572120332.0),
    List(22, 1572120346.0, 1572120359.0),
    List(23, 1572120322.0, 1572120325.0),
    List(24, 1572120321.0, 1572120321.0),
    List(25, 1572120320.0, 1572120329.0),
    List(26, 1572120320.0, 1572120323.0),
    List(27, 1572120331.0, 1572120331.0),
    List(28, 1572120320.0, 1572120343.0),
    List(28, 1572120345.0, 1572120367.0),
    List(29, 1572120321.0, 1572120344.0),
    List(30, 1572120320.0, 1572120320.0),
    List(30, 1572120339.0, 1572120339.0),
    List(31, 1572120332.0, 1572120332.0),
    List(31, 1572120346.0, 1572120359.0),
    List(32, 1572120322.0, 1572120325.0),
    List(33, 1572120321.0, 1572120321.0),
    List(34, 1572120320.0, 1572120329.0),
    List(35, 1572120320.0, 1572120323.0),
    List(38, 1572120346.0, 1572120352.0),
    List(39, 1572120353.0, 1572120356.0),
    List(40, 1572120361.0, 1572120361.0),
    List(41, 1572120320.0, 1572120320.0),
    List(42, 1572120357.0, 1572120360.0),

  )

  val wideInputConf = JDBCInputConf(
    sourceId = 100,
    jdbcUrl = clickhouseContainer.jdbcUrl,
    query = "SELECT * FROM `2te116u_tmy_test_simple_rules` ORDER BY ts",
    driverName = clickhouseContainer.driverName,
    datetimeField = 'ts,
    eventsMaxGapMs = 60000L,
    defaultEventsGapMs = 1000L,
    chunkSizeMs = Some(900000L),
    partitionFields = Seq('loco_num, 'section, 'upload_id)
  )

  val narrowInputConf = JDBCInputConf(
    sourceId = 200,
    jdbcUrl = clickhouseContainer.jdbcUrl,
    query = "SELECT * FROM math_test ORDER BY dt",
    driverName = clickhouseContainer.driverName,
    datetimeField = 'dt,
    eventsMaxGapMs = 60000L,
    defaultEventsGapMs = 1000L,
    chunkSizeMs = Some(900000L),
    partitionFields = Seq('loco_num, 'section, 'upload_id),
    dataTransformation = Some(NarrowDataUnfolding('sensor_id, 'value_float, Map.empty, Some(Map.empty), Some(1000))),
  )

  val influxInputConf = InfluxDBInputConf(
    sourceId = 300,
    url = influxContainer.url,
    query = "SELECT * FROM \"2te116u_tmy_test_simple_rules\" ORDER BY time",
    userName = Some("default"),
    password = Some("default"),
    dbName = influxContainer.dbName,
    eventsMaxGapMs = 60000L,
    defaultEventsGapMs = 1000L,
    chunkSizeMs = Some(900000L),
    partitionFields = Seq('loco_num, 'section, 'upload_id),
    additionalTypeChecking = Some(false)
  )

  val narrowInputIvolgaConf = JDBCInputConf(
    sourceId = 400,
    jdbcUrl = clickhouseContainer.jdbcUrl,
    query = "SELECT * FROM ivolga_test_narrow ORDER BY dt",
    driverName = clickhouseContainer.driverName,
    datetimeField = 'dt,
    eventsMaxGapMs = 60000L,
    defaultEventsGapMs = 1000L,
    chunkSizeMs = Some(900000L),
    partitionFields = Seq('stock_num,'upload_id),
    dataTransformation = Some(NarrowDataUnfolding('sensor_id, 'value_float, Map.empty, Some(Map('value_str -> List('SOC_2_UKV1_UOVS))), Some(15000L))),
  )

  val wideInputIvolgaConf = JDBCInputConf(
    sourceId = 500,
    jdbcUrl = clickhouseContainer.jdbcUrl,
    query = "SELECT * FROM `ivolga_test_wide` ORDER BY ts",
    driverName = clickhouseContainer.driverName,
    datetimeField = 'ts,
    eventsMaxGapMs = 60000L,
    defaultEventsGapMs = 1000L,
    chunkSizeMs = Some(900000L),
    partitionFields = Seq('stock_num, 'upload_id),
    dataTransformation = Some(WideDataFilling(
      Map.empty, defaultTimeout = Some(15000L))
    )
  )

  val wideRowSchema =
    RowSchema('series_storage, 'from, 'to, ('app, 1), 'id, 'timestamp, 'context, wideInputConf.partitionFields)

  val narrowRowSchema =
    RowSchema('series_storage, 'from, 'to, ('app, 2), 'id, 'timestamp, 'context, narrowInputConf.partitionFields)

  val influxRowSchema =
    RowSchema('series_storage, 'from, 'to, ('app, 3), 'id, 'timestamp, 'context, influxInputConf.partitionFields)

  val narrowIvolgaRowSchema =
    RowSchema('series_storage, 'from, 'to, ('app, 4), 'id, 'timestamp, 'context, narrowInputIvolgaConf.partitionFields)

  val wideIvolgaRowSchema =
    RowSchema('series_storage, 'from, 'to, ('app, 5), 'id, 'timestamp, 'context, wideInputIvolgaConf.partitionFields)

  val chConnection = s"jdbc:clickhouse://localhost:$port/default"
  val chDriver = "ru.yandex.clickhouse.ClickHouseDriver"

  val wideOutputConf = JDBCOutputConf(
    "events_wide_test",
    wideRowSchema,
    chConnection,
    chDriver
  )

  val narrowOutputConf = JDBCOutputConf(
    "events_narrow_test",
    narrowRowSchema,
    chConnection,
    chDriver
  )

  val influxOutputConf = JDBCOutputConf(
    "events_influx_test",
    influxRowSchema,
    chConnection,
    chDriver
  )

  val narrowOutputIvolgaConf = JDBCOutputConf(
    "events_narrow_ivolga_test",
    narrowIvolgaRowSchema,
    chConnection,
    chDriver
  )

  val wideOutputIvolgaConf = JDBCOutputConf(
    "events_wide_ivolga_test",
    narrowIvolgaRowSchema,
    chConnection,
    chDriver
  )

  override def afterStart(): Unit = {
    super.afterStart()

    val chScripts: Seq[String] = Seq(
      "/sql/test/cases-narrow-schema-new.sql",
      "/sql/test/cases-wide-schema-new.sql",
      "/sql/test/cases-narrow-schema-ivolga.sql",
      "/sql/test/cases-wide-schema-ivolga.sql",
      "/sql/test/cases-sinks-schema.sql"
    )

    chScripts.foreach(elem => {

      Files.readResource(elem)
           .mkString
           .split(";")
           .foreach(clickhouseContainer.executeUpdate)

    })

    Files
      .readResource("/sql/infl-test-db-schema.sql")
      .mkString
      .split(";")
      .foreach(influxContainer.executeQuery)
    
    Files
      .readResource("/sql/test/cases-narrow-new.influx")
      .mkString
      .split(";")
      .foreach(influxContainer.executeUpdate)
      
    val insertInfo = Seq(
      ("math_test", "/sql/test/cases-narrow-new.csv"),
      ("ivolga_test_narrow", "/sql/test/cases-narrow-ivolga.csv"),
      ("ivolga_test_wide", "/sql/test/cases-wide-ivolga.csv"),
      ("`2te116u_tmy_test_simple_rules`", "/sql/test/cases-wide-new.csv")
    )

    insertInfo.foreach(elem => {

      val insertData = Files.readResource(elem._2)
                            .drop(1)
                            .mkString("\n")
                            
      clickhouseContainer.executeUpdate(s"INSERT INTO ${elem._1} FORMAT CSV\n${insertData}")

    })

  }

  val firstValidationQuery = """
       SELECT number, c 
       FROM (
         SELECT number FROM numbers(%s)
       ) 
       LEFT JOIN (
         SELECT id, COUNT(id) AS c FROM %s GROUP BY id
       ) e ON number = e.id ORDER BY number
  """

  val secondValidationQuery = "SELECT id, from, to FROM %s ORDER BY id, from, to"

  override def afterAll(): Unit = {
    super.afterAll()
    clickhouseContainer.stop()
    influxContainer.stop()
    container.stop()
  }

  "Data" should "load properly" in {
    checkByQuery(List(List(27.0)), "SELECT COUNT(*) FROM `2te116u_tmy_test_simple_rules`")
    checkByQuery(List(List(81.0)), "SELECT COUNT(*) FROM math_test")
    checkInfluxByQuery(List(List(27.0, 27.0, 27.0)), "SELECT COUNT(*) FROM \"2te116u_tmy_test_simple_rules\"")
  }

  "Cases 1-17" should "work in wide table" in {
    (1 to 17).foreach { id =>
      Post(
        "/streamJob/from-jdbc/to-jdbc/?run_async=0",
        FindPatternsRequest(s"17wide_$id", wideInputConf, wideOutputConf, List(casesPatterns(id - 1)))
      ) ~>
      route ~> check {
        withClue(s"Pattern ID: $id") {
          status shouldEqual StatusCodes.OK
        }
        //checkByQuery(List(List(id.toDouble, incidentsCount(id).toDouble)), s"SELECT $id, COUNT(*) FROM events_wide_test WHERE id = $id")
      }
    }
    checkByQuery(
      incidentsCount
        .map {
          case (k, v) => List(k.toDouble, v.toDouble)
        }
        .toList
        .sortBy(_.head),
      firstValidationQuery.format("1, 17", "events_wide_test")
    )
    checkByQuery(incidentsTimestamps, secondValidationQuery.format("events_wide_test"))
  }

  "Cases 1-17" should "work in narrow table" in {
    (1 to 17).foreach { id =>
      Post(
        "/streamJob/from-jdbc/to-jdbc/?run_async=0",
        FindPatternsRequest(s"17narrow_$id", narrowInputConf, narrowOutputConf, List(casesPatterns(id - 1)))
      ) ~>
      route ~> check {
        withClue(s"Pattern ID: $id") {
          status shouldEqual StatusCodes.OK
        }
      }
    }
    checkByQuery(
      incidentsCount
        .map {
          case (k, v) => List(k.toDouble, v.toDouble)
        }
        .toList
        .sortBy(_.head),
      firstValidationQuery.format("1, 17", "events_narrow_test")
    )
    checkByQuery(incidentsTimestamps, secondValidationQuery.format("events_narrow_test"))
  }

  "Cases 1-17" should "work in influx table" in {
    (1 to 17).foreach { id =>
      Post(
        "/streamJob/from-influxdb/to-jdbc/?run_async=0",
        FindPatternsRequest(s"17influx_$id", influxInputConf, influxOutputConf, List(casesPatterns(id - 1)))
      ) ~>
      route ~> check {
        withClue(s"Pattern ID: $id") {
          status shouldEqual StatusCodes.OK
        }
      }
    }
    checkByQuery(
      incidentsCount
        .map {
          case (k, v) => List(k.toDouble, v.toDouble)
        }
        .toList
        .sortBy(_.head),
      firstValidationQuery.format("1, 17", "events_influx_test")
    )
    checkByQuery(incidentsTimestamps, secondValidationQuery.format("events_influx_test"))
  }

  "Cases 18-42" should "work in ivolga wide table" in {
    (18 to 42).foreach { id =>
      Post(
        "/streamJob/from-jdbc/to-jdbc/?run_async=0",
        FindPatternsRequest(s"17wide_$id", wideInputIvolgaConf, wideOutputIvolgaConf, List(casesPatterns(id - 1)))
      ) ~>
        route ~> check {
        withClue(s"Pattern ID: $id") {
          status shouldEqual StatusCodes.OK
        }
      }
    }
    checkByQuery(
      incidentsIvolgaCount
        .map {
          case (k, v) => List(k.toDouble, v.toDouble)
        }
        .toList
        .sortBy(_.head),
      firstValidationQuery.format("18, 25", "events_wide_ivolga_test")
    )
    checkByQuery(incidentsIvolgaTimestamps, secondValidationQuery.format("events_wide_ivolga_test"))
  }

  "Cases 18-42" should "work in ivolga narrow table" in {
    (18 to 42).foreach { id =>
      Post(
        "/streamJob/from-jdbc/to-jdbc/?run_async=0",
        FindPatternsRequest(s"17narrow_$id", narrowInputIvolgaConf, narrowOutputIvolgaConf, List(casesPatterns(id - 1)))
      ) ~>
        route ~> check {
        withClue(s"Pattern ID: $id") {
          status shouldEqual StatusCodes.OK
        }
      }
    }
    checkByQuery(
      incidentsIvolgaCount
        .map {
          case (k, v) => List(k.toDouble, v.toDouble)
        }
        .toList
        .sortBy(_.head),
      firstValidationQuery.format("18, 25", "events_narrow_ivolga_test")
    )
    checkByQuery(incidentsIvolgaTimestamps, secondValidationQuery.format("events_wide_narrow_test"))
  }
}
