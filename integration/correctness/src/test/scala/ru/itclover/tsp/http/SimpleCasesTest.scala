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

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.util.{Success, Try}

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

  var fileSourceString = ""
  val filesPath = "integration/correctness/src/test/resources/simple_cases"

  val patternsPath = s"${filesPath}/core/patterns.json"

  val patternsString: Try[String] = Files.readFile(patternsPath)

  patternsString match {
    case Success(some) => {
      fileSourceString = some
    }
  }

  var jsonObject = fileSourceString.parseJson
  val casesPatterns = jsonObject.convertTo[Seq[RawPattern]].map(p => (p.id -> p)).toMap

  var fileSourceStringIvolga = ""
  val patternsPathIvolga = s"${filesPath}/ivolga/patterns.json"

  val patternsStringIvolga: Try[String] = Files.readFile(patternsPathIvolga)

  patternsStringIvolga match {
    case Success(some) => {
      fileSourceStringIvolga = some
    }
  }

  var jsonObjectIvolga = fileSourceStringIvolga.parseJson
  val casesPatternsIvolga = jsonObjectIvolga.convertTo[Seq[RawPattern]].map(p => (p.id -> p)).toMap

  val coreIncidentsPath = s"${filesPath}/core/incidents.json"
  val incidentsString: Try[String] = Files.readFile(coreIncidentsPath)

  incidentsString match {
    case Success(some) => {
      fileSourceString = some
    }
  }

  jsonObject = fileSourceString.parseJson
  val coreRawIncidents = jsonObject.convertTo[Map[String, String]]

  val incidentsCount = coreRawIncidents.map{ case (k ,v) => (k.toInt, v.toInt)}

  val ivolgaIncidentsPath = s"${filesPath}/ivolga/incidents.json"
  val ivolgaIncidentsString: Try[String] = Files.readFile(ivolgaIncidentsPath)

  ivolgaIncidentsString match {
    case Success(some) => {
      fileSourceString = some
    }
  }

  jsonObject = fileSourceString.parseJson
  val ivolgaIncidents = jsonObject.convertTo[Map[String, String]]

  val incidentsIvolgaCount = ivolgaIncidents.map{ case (k ,v) => (k.toInt, v.toInt)}

  val rawIncidentsTimestamps: ListBuffer[List[Double]] = ListBuffer.empty

  Files.readResource("/simple_cases/core/timestamps.csv")
       .foreach(elem => {

          val elements = elem.split(",")
          rawIncidentsTimestamps += List(
            elements(0).toDouble,
            elements(1).toDouble,
            elements(2).toDouble
          )   

       }) 

  // type is explicitly specified to avoid writing pattern ID as Double
  val incidentsTimestamps: List[List[Double]] = rawIncidentsTimestamps.toList

  val ivolgaIncidentsTimestamps: ListBuffer[List[Double]] = ListBuffer.empty

  Files.readResource("/simple_cases/ivolga/timestamps.csv")
       .foreach(elem => {

          val elements = elem.split(",")
          ivolgaIncidentsTimestamps += List(
            elements(0).toDouble,
            elements(1).toDouble,
            elements(2).toDouble
          )   

       }) 

  val incidentsIvolgaTimestamps: List[List[Double]] = ivolgaIncidentsTimestamps.toList

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

  val narrowInputConf = wideInputConf.copy(
    sourceId = 200,
    query = "SELECT * FROM math_test ORDER BY dt",
    datetimeField = 'dt,
    dataTransformation = Some(NarrowDataUnfolding('sensor_id, 'value_float, Map.empty, Some(Map.empty), Some(1000))),
  )

  val narrowInputIvolgaConf = wideInputConf.copy(
    sourceId = 400,
    query = "SELECT * FROM ivolga_test_narrow ORDER BY dt",
    datetimeField = 'dt,
    partitionFields = Seq('stock_num,'upload_id),
    dataTransformation = Some(
      NarrowDataUnfolding('sensor_id, 'value_float, Map.empty, Some(Map('value_str -> List('SOC_2_UKV1_UOVS))), Some(15000L))
    ),
  )

  val wideInputIvolgaConf = wideInputConf.copy(
    sourceId = 500,
    query = "SELECT * FROM `ivolga_test_wide` ORDER BY ts",
    partitionFields = Seq('stock_num, 'upload_id),
    dataTransformation = Some(WideDataFilling(
      Map.empty, defaultTimeout = Some(15000L))
    )
  )

  val wideRowSchema = RowSchema(
    sourceIdField = 'series_storage,
    fromTsField = 'from,
    toTsField = 'to,
    appIdFieldVal = ('app, 1),
    patternIdField = 'id,
    processingTsField = 'timestamp,
    contextField = 'context,
    forwardedFields = wideInputConf.partitionFields
  )

  val narrowRowSchema = wideRowSchema.copy(
    appIdFieldVal = ('app, 2),
    forwardedFields = narrowInputConf.partitionFields
  )

  val influxRowSchema = wideRowSchema.copy(
    appIdFieldVal = ('app, 3),
    forwardedFields = influxInputConf.partitionFields
  )

  val narrowIvolgaRowSchema = wideRowSchema.copy(
    appIdFieldVal = ('app, 4),
    forwardedFields = narrowInputIvolgaConf.partitionFields
  )

  val wideIvolgaRowSchema = wideRowSchema.copy(
    appIdFieldVal = ('app, 5),
    forwardedFields = wideInputIvolgaConf.partitionFields
  )

  val chConnection = s"jdbc:clickhouse://localhost:$port/default"
  val chDriver = "ru.yandex.clickhouse.ClickHouseDriver"

  val wideOutputConf = JDBCOutputConf(
    tableName = "events_wide_test",
    rowSchema = wideRowSchema,
    jdbcUrl = chConnection,
    driverName = chDriver
  )

  val narrowOutputConf = wideOutputConf.copy(
    tableName = "events_narrow_test",
    rowSchema = narrowRowSchema
  )

  val influxOutputConf = wideOutputConf.copy(
    tableName = "events_influx_test",
    rowSchema = influxRowSchema
  )

  val narrowOutputIvolgaConf = wideOutputConf.copy(
    tableName = "events_narrow_ivolga_test",
    rowSchema = narrowIvolgaRowSchema
  )

  val wideOutputIvolgaConf = wideOutputConf.copy(
    tableName = "events_wide_ivolga_test",
    rowSchema = wideIvolgaRowSchema
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

  def firstValidationQuery(table: String, numbers: Seq[Range]) = s"""
       SELECT number, c 
       FROM (
         ${numbers.map(r => s"SELECT number FROM numbers(${r.start}, ${r.size})").mkString(" UNION ALL ")}
       ) 
       LEFT JOIN (
         SELECT id, COUNT(id) AS c FROM ${table} GROUP BY id
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
    checkByQuery(List(List(53.0)), "SELECT COUNT(*) FROM `2te116u_tmy_test_simple_rules`")
    checkByQuery(List(List(159.0)), "SELECT COUNT(*) FROM math_test")
    checkInfluxByQuery(List(List(53.0, 53.0, 53.0)), "SELECT COUNT(*) FROM \"2te116u_tmy_test_simple_rules\"")
  }

  "Cases 1-17, 43-50" should "work in wide table" in {
    casesPatterns.keys.foreach { id =>
      Post(
        "/streamJob/from-jdbc/to-jdbc/?run_async=0",
        FindPatternsRequest(s"17wide_$id", wideInputConf, wideOutputConf, List(casesPatterns(id)))
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
      firstValidationQuery("events_wide_test", numbersToRanges(casesPatterns.keys.map(_.toInt).toList.sorted))
    )
    checkByQuery(incidentsTimestamps, secondValidationQuery.format("events_wide_test"))
  }

  "Cases 1-17, 43-50" should "work in narrow table" in {
    casesPatterns.keys.foreach { id =>
      Post(
        "/streamJob/from-jdbc/to-jdbc/?run_async=0",
        FindPatternsRequest(s"17narrow_$id", narrowInputConf, narrowOutputConf, List(casesPatterns(id)))
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
      firstValidationQuery("events_narrow_test", numbersToRanges(casesPatterns.keys.map(_.toInt).toList.sorted))
    )
    checkByQuery(incidentsTimestamps, secondValidationQuery.format("events_narrow_test"))
  }

  "Cases 1-17, 43-50" should "work in influx table" in {
    casesPatterns.keys.foreach { id =>
      Post(
        "/streamJob/from-influxdb/to-jdbc/?run_async=0",
        FindPatternsRequest(s"17influx_$id", influxInputConf, influxOutputConf, List(casesPatterns(id)))
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
      firstValidationQuery("events_influx_test", numbersToRanges(casesPatterns.keys.map(_.toInt).toList.sorted))
    )
    checkByQuery(incidentsTimestamps, secondValidationQuery.format("events_influx_test"))
  }

  "Cases 18-42" should "work in ivolga wide table" in {
    casesPatternsIvolga.keys.foreach { id =>
      Post(
        "/streamJob/from-jdbc/to-jdbc/?run_async=0",
        FindPatternsRequest(s"17wide_$id", wideInputIvolgaConf, wideOutputIvolgaConf, List(casesPatternsIvolga(id)))
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
      firstValidationQuery("events_wide_ivolga_test", numbersToRanges(casesPatternsIvolga.keys.map(_.toInt).toList.sorted))
    )
    checkByQuery(incidentsIvolgaTimestamps, secondValidationQuery.format("events_wide_ivolga_test"))
  }

  "Cases 18-42" should "work in ivolga narrow table" in {
    casesPatternsIvolga.keys.foreach { id =>
      Post(
        "/streamJob/from-jdbc/to-jdbc/?run_async=0",
        FindPatternsRequest(s"17narrow_$id", narrowInputIvolgaConf, narrowOutputIvolgaConf, List(casesPatternsIvolga(id)))
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
      firstValidationQuery("events_narrow_ivolga_test", numbersToRanges(casesPatternsIvolga.keys.map(_.toInt).toList.sorted))
    )
    checkByQuery(incidentsIvolgaTimestamps, secondValidationQuery.format("events_narrow_ivolga_test"))
  }

  def numbersToRanges(numbers: List[Int]): List[Range] = {
    @tailrec
    def inner(in: List[Int], acc: List[Range]): List[Range] = (in, acc) match {
      case (Nil, a) => a.reverse
      case (n :: tail, r :: tt) if n == r.end + 1 => inner(tail, (r.start to n) :: tt)
      case (n :: tail, a) => inner(tail, (n to n) :: a)
    }

    inner(numbers, Nil)
  }
}
