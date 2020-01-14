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
import ru.itclover.tsp.utils.CollectionsOps._
import spray.json._

import scala.util.{Try,Success,Failure}
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

  /* 
  val casesPatterns = Seq(
    RawPattern("1", "PowerPolling = 50"),
    RawPattern("2", "PowerPolling > 20"),
    RawPattern("3", "PowerPolling >= 20"),
    RawPattern("4", "PowerPolling < 20"),
    RawPattern("5", "PowerPolling <= 20"),
    RawPattern("6", "PowerPolling <> 70"),
    RawPattern("7", "POilDieselOut > 9.52 and SpeedThrustMin = 51"),
    RawPattern("8", "wait(2 sec, POilDieselOut >= 9.52 for 2 sec)"),
    RawPattern("9", "POilDieselOut <= 9.53 and SpeedThrustMin =51 andThen SpeedThrustMin = 52"),
    RawPattern("10", "POilDieselOut = 9.53 or SpeedThrustMin = 51"),
    RawPattern("11", "POilDieselOut < 9.50 until SpeedThrustMin > 51"),
    RawPattern("12", "abs(SpeedThrustMin + POilDieselOut) > 40"),
    RawPattern("13", "avg(SpeedThrustMin, 2 sec) = 22"),
    RawPattern("14", "avgOf(POilDieselOut, SpeedThrustMin) > 0"),
    RawPattern("15", "lag(POilDieselOut) < 0"),
    RawPattern("16", "wait(1 sec, SpeedThrustMin = 0 for 1 sec andThen SpeedThrustMin > 40)"),
    RawPattern("17", "abs(POilDieselOut - 9.53) < 0.001 andThen Wait(3 sec, POilDieselOut < 9.50 for 3 sec)"),
    RawPattern("18", "car_2_TCU_out_E_Bog = 585"),
    RawPattern("19", "car_2_TCU_out_E_Bog != 511"),
    RawPattern("20", "car_2_TCU_out_E_Bog < 700"),
    RawPattern("21", "car_2_TCU_out_E_Bog > 690"),
    RawPattern("22", "car_2_TCU_out_E_Bog < 600 for 2 sec"),
    RawPattern("23", "car_2_TCU_out_E_Bog > 600 for 2 sec andThen car_2_TCU_out_E_Bog < 600"),
    RawPattern("24", "car_2_TCU_out_E_Bog = 530 and lag(car_2_TCU_out_E_Bog) = 700"),
    RawPattern("25", "car_2_BCU_out_Indirect_Brake_Active = 1 for 9 sec andThen car_2_BCU_out_Indirect_Brake_Active = 0"),
    RawPattern("26", "car_2_TCU_out_E_Bog = 700 and car_2_BCU_out_Indirect_Brake_Active = 1 andThen Wait(3 sec, car_2_BCU_out_Indirect_Brake_Active = 1 for 3 sec)"),
    RawPattern("27", "car_4_TCU_out_E_Bog = 585"),
    RawPattern("28", "car_4_TCU_out_E_Bog != 511"),
    RawPattern("29", "car_4_TCU_out_E_Bog < 700"),
    RawPattern("30", "car_4_TCU_out_E_Bog > 690"),
    RawPattern("31", "car_4_TCU_out_E_Bog < 600 for 2 sec"),
    RawPattern("32", "car_4_TCU_out_E_Bog > 600 for 2 sec andThen car_4_TCU_out_E_Bog < 600"),
    RawPattern("33", "car_4_TCU_out_E_Bog = 530 and lag(car_4_TCU_out_E_Bog) = 700"),
    RawPattern("34", "car_4_BCU_out_Indirect_Brake_Active = 1 for 9 sec andThen car_4_BCU_out_Indirect_Brake_Active = 0"),
    RawPattern("35", "car_4_TCU_out_E_Bog = 700 and car_2_BCU_out_Indirect_Brake_Active = 1 andThen Wait(3 sec, car_4_BCU_out_Indirect_Brake_Active = 1 for 3 sec)"),
    RawPattern("36", "ABKM_Brake_Pos > 1"),
    RawPattern("37", "ABKM_Brake_Fail = 0"),
    RawPattern("38", "PSN_1_is_working != 0 and PSN_1_HV_INPUT_VOLTAGE > 2000 and PSN_1_HV_OUTPUT_VOLTAGE > 450 andThen wait(5 sec, PSN_1_is_working != 0 and PSN_1_HV_INPUT_VOLTAGE > 2000 and PSN_1_HV_OUTPUT_VOLTAGE < 450 for 5 sec)"),
    RawPattern("39", "wait(3 sec, PSN_1_HV_INPUT_VOLTAGE > 2000 and PSN_1_is_working  > 0 and PSN_1_CHARGER_CHARGER_CURRENT < 0.5 for 3 sec)"),
    // for str value
    RawPattern("40", "SOC_2_UKV1_UOVS = 'OFF' and lag(SOC_2_UKV1_UOVS) = 'FAILURE'"),
    RawPattern("41", "car_2_TCU_out_E_Bog = 700 and SOC_2_UKV1_UOVS = 'OFF'"),
    RawPattern("42", "Wait(3 sec, SOC_2_UKV1_UOVS = 'FAILURE' for 3 sec)"),
  )
  **/

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

  val influxOutputConf = JDBCOutputConf(
    "events_influx_test",
    influxRowSchema,
    s"jdbc:clickhouse://localhost:$port/default",
    "ru.yandex.clickhouse.ClickHouseDriver"
  )

  val narrowOutputIvolgaConf = JDBCOutputConf(
    "events_narrow_ivolga_test",
    narrowIvolgaRowSchema,
    s"jdbc:clickhouse://localhost:$port/default",
    "ru.yandex.clickhouse.ClickHouseDriver"
  )

  val wideOutputIvolgaConf = JDBCOutputConf(
    "events_wide_ivolga_test",
    narrowIvolgaRowSchema,
    s"jdbc:clickhouse://localhost:$port/default",
    "ru.yandex.clickhouse.ClickHouseDriver"
  )

  override def afterStart(): Unit = {
    super.afterStart()

    Files
      .readResource("/sql/test/cases-narrow-schema-new.sql")
      .mkString
      .split(";")
      .foreach(clickhouseContainer.executeUpdate)

    Files
      .readResource("/sql/test/cases-wide-schema-new.sql")
      .mkString
      .split(";")
      .foreach(clickhouseContainer.executeUpdate)

    Files
      .readResource("/sql/infl-test-db-schema.sql")
      .mkString
      .split(";")
      .foreach(influxContainer.executeQuery)

    Files
      .readResource("/sql/test/cases-narrow-schema-ivolga.sql")
      .mkString
      .split(";")
      .foreach(clickhouseContainer.executeUpdate)

    Files
      .readResource("/sql/test/cases-wide-schema-ivolga.sql")
      .mkString
      .split(";")
      .foreach(clickhouseContainer.executeUpdate)

    val mathCSVData = Files
      .readResource("/sql/test/cases-narrow-new.csv")
      .drop(1)
      .mkString("\n")

    val rulesCSVData = Files
      .readResource("/sql/test/cases-wide-new.csv")
      .drop(1)
      .mkString("\n")

    val ivolgaNarrowCSVData = Files
      .readResource("/sql/test/cases-narrow-ivolga.csv")
      .drop(1)
      .mkString("\n")

    val ivolgaWideCSVData = Files
      .readResource("/sql/test/cases-wide-ivolga.csv")
      .drop(1)
      .mkString("\n")


    Files
      .readResource("/sql/test/cases-narrow-new.influx")
      .mkString
      .split(";")
      .foreach(influxContainer.executeUpdate)

    clickhouseContainer.executeUpdate(s"INSERT INTO math_test FORMAT CSV\n${mathCSVData}")

    clickhouseContainer.executeUpdate(s"INSERT INTO ivolga_test_narrow FORMAT CSV\n${ivolgaNarrowCSVData}")

    clickhouseContainer.executeUpdate(s"INSERT INTO ivolga_test_wide FORMAT CSV\n${ivolgaWideCSVData}")

    clickhouseContainer.executeUpdate(s"INSERT INTO `2te116u_tmy_test_simple_rules` FORMAT CSV\n${rulesCSVData}")

    Files
      .readResource("/sql/test/cases-sinks-schema.sql")
      .mkString
      .split(";")
      .foreach(clickhouseContainer.executeUpdate)

  }

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
      s"SELECT number, c FROM (SELECT number FROM numbers(1, 17)) LEFT JOIN (SELECT id, COUNT(id) AS c FROM events_wide_test GROUP BY id) e ON number = e.id ORDER BY number"
    )
    checkByQuery(incidentsTimestamps, "SELECT id, from, to FROM events_wide_test ORDER BY id, from, to")
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
      s"SELECT number, c FROM (SELECT number FROM numbers(1, 17)) LEFT JOIN (SELECT id, COUNT(id) AS c FROM events_narrow_test GROUP BY id) e ON number = e.id ORDER BY number"
    )
    checkByQuery(incidentsTimestamps, "SELECT id, from, to FROM events_narrow_test ORDER BY id, from, to")
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
      s"SELECT number, c FROM (SELECT number FROM numbers(1, 17)) LEFT JOIN (SELECT id, COUNT(id) AS c FROM events_influx_test GROUP BY id) e ON number = e.id ORDER BY number"
    )
    checkByQuery(incidentsTimestamps, "SELECT id, from, to FROM events_influx_test ORDER BY id, from, to")
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
      s"SELECT number, c FROM (SELECT number FROM numbers(18, 25)) LEFT JOIN (SELECT id, COUNT(id) AS c FROM events_wide_ivolga_test GROUP BY id) e ON number = e.id ORDER BY number"
    )
    checkByQuery(incidentsIvolgaTimestamps, "SELECT id, from, to FROM events_wide_ivolga_test ORDER BY id, from, to")
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
      s"SELECT number, c FROM (SELECT number FROM numbers(18, 25)) LEFT JOIN (SELECT id, COUNT(id) AS c FROM events_narrow_ivolga_test GROUP BY id) e ON number = e.id ORDER BY number"
    )
    checkByQuery(incidentsIvolgaTimestamps, "SELECT id, from, to FROM events_narrow_ivolga_test ORDER BY id, from, to")
  }
}
