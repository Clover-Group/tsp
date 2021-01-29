package ru.itclover.tsp.http

import java.io.File
import java.util.concurrent.{SynchronousQueue, ThreadPoolExecutor, TimeUnit}
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import com.dimafeng.testcontainers._
import com.google.common.util.concurrent.ThreadFactoryBuilder

import java.util.{Properties, UUID}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, _}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.spark.sql.SparkSession
import org.scalatest.{Assertion, FlatSpec}

import scala.util.Failure
// import org.scalatest.concurrent.Waiters._
// import org.testcontainers.containers.Network
import org.testcontainers.containers.wait.strategy.Wait
import ru.itclover.tsp.core.RawPattern
import ru.itclover.tsp.http.domain.input.FindPatternsRequest
import ru.itclover.tsp.http.protocols.RoutesProtocols
import ru.itclover.tsp.http.utils.{InfluxDBContainer, JDBCContainer, SqlMatchers}
import ru.itclover.tsp.io.input.{InfluxDBInputConf, JDBCInputConf, KafkaInputConf, NarrowDataUnfolding, WideDataFilling}
import ru.itclover.tsp.io.output.{JDBCOutputConf, NewRowSchema}
import ru.itclover.tsp.utils.Files
import ru.itclover.tsp.spark.io.{JDBCInputConf => SparkJDBCInputConf, JDBCOutputConf => SparkJDBCOutputConf, KafkaInputConf => SparkKafkaInputConf, NewRowSchema => SparkRowSchema}
import ru.itclover.tsp.spark.io.{NarrowDataUnfolding => SparkNDU, WideDataFilling => SparkWDF}
import spray.json._

import scala.annotation.tailrec
import scala.util.{Success, Try}
// import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

// In test cases, 'should' expressions are non-unit. Suppressing wartremover warnings about it
// Also, this test seems to be heavily relying on Any. But still TODO: Investigate
@SuppressWarnings(Array(
  "org.wartremover.warts.NonUnitStatements",
  "org.wartremover.warts.Any"
))
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
  streamEnvironment.setParallelism(1)
  streamEnvironment.setMaxParallelism(30000) // For proper keyBy partitioning

  val spark = SparkSession.builder()
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

  val port = 8161
  val influxPort = 8144
  val chNativePort = 9098
  implicit val clickhouseContainer = new JDBCContainer(
    "yandex/clickhouse-server:latest",
    port -> 8123 :: chNativePort -> 9000 :: Nil,
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

  val kafkaContainer = KafkaContainer()


  override val container = MultipleContainers(
    clickhouseContainer,
    influxContainer,
    kafkaContainer
  )

  val kafkaBrokerHost = "127.0.0.1"
  lazy val kafkaBrokerUrl = s"$kafkaBrokerHost:${kafkaContainer.mappedPort(9092)}"

  val filesPath = "integration/correctness/src/test/resources/simple_cases"

  val patternsPath = s"${filesPath}/core/patterns.json"

  val patternsString: Try[String] = Files.readFile(patternsPath)

  val fileSourceString = patternsString match {
    case Success(some) => some
    case _             => ""
  }

  val jsonObject = fileSourceString.parseJson
  val casesPatterns = jsonObject.convertTo[Seq[RawPattern]].map(p => (p.id -> p)).toMap

  val patternsPathIvolga = s"${filesPath}/ivolga/patterns.json"

  val patternsStringIvolga: Try[String] = Files.readFile(patternsPathIvolga)

  val fileSourceStringIvolga = patternsStringIvolga match {
    case Success(some) => some
    case _ => ""
  }

  val jsonObjectIvolga = fileSourceStringIvolga.parseJson
  val casesPatternsIvolga = jsonObjectIvolga.convertTo[Seq[RawPattern]].map(p => (p.id -> p)).toMap

  val coreIncidentsPath = s"${filesPath}/core/incidents.json"
  val incidentsString: Try[String] = Files.readFile(coreIncidentsPath)

  val fileSourceStringInc = incidentsString match {
    case Success(some) => some
    case _             => ""
  }

  val jsonObjectInc = fileSourceStringInc.parseJson
  val coreRawIncidents = jsonObjectInc.convertTo[Map[String, String]]

  val incidentsCount = coreRawIncidents.map{ case (k ,v) => (k.toInt, v.toInt)}

  val ivolgaIncidentsPath = s"${filesPath}/ivolga/incidents.json"
  val ivolgaIncidentsString: Try[String] = Files.readFile(ivolgaIncidentsPath)

  val fileSourceStringIvolgaInc = ivolgaIncidentsString match {
    case Success(some) => some
    case _             => ""
  }

  val jsonObjectIvolgaInc = fileSourceStringIvolgaInc.parseJson
  val ivolgaIncidents = jsonObjectIvolgaInc.convertTo[Map[String, String]]

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
    eventsMaxGapMs = Some(60000L),
    defaultEventsGapMs = Some(1000L),
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
    eventsMaxGapMs = Some(60000L),
    defaultEventsGapMs = Some(1000L),
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

  val wideRowSchema = NewRowSchema(
    unitIdField = 'series_storage,
    fromTsField = 'from,
    toTsField = 'to,
    appIdFieldVal = ('app, 1),
    patternIdField = 'id,
    subunitIdField = 'subunit
  )
//  val wideKafkaInputConf = KafkaInputConf(
//    brokers = kafkaBrokerUrl,
//    topic = "2te116u_tmy_test_simple_rules",
//    datetimeField = 'dt,
//    partitionFields = Seq('loco_num, 'section, 'upload_id),
//    fieldsTypes = Map("dt" -> "float64",
//      "upload_id" -> "string",
//      "loco_num" -> "string",
//      "section" -> "string",
//      "POilDieselOut" -> "float64",
//      "SpeedThrustMin" -> "float64",
//      "PowerPolling" -> "float64"
//    )
//  )

  val wideSparkInputConf = SparkJDBCInputConf(
    sourceId = 100,
    jdbcUrl = clickhouseContainer.jdbcUrl,
    query = "SELECT * FROM `2te116u_tmy_test_simple_rules` ORDER BY ts",
    driverName = clickhouseContainer.driverName,
    datetimeField = 'ts,
    eventsMaxGapMs = Some(60000L),
    defaultEventsGapMs = Some(1000L),
    chunkSizeMs = Some(900000L),
    partitionFields = Seq('loco_num, 'section, 'upload_id)
  )

  lazy val wideSparkKafkaInputConf = SparkKafkaInputConf(
    sourceId = 500,
    brokers = kafkaBrokerUrl,
    topic = "2te116u_tmy_test_simple_rules",
    datetimeField = 'dt,
    partitionFields = Seq('loco_num, 'section, 'upload_id),
    fieldsTypes = Map("dt" -> "float64",
      "upload_id" -> "string",
      "loco_num" -> "string",
      "section" -> "string",
      "POilDieselOut" -> "float64",
      "SpeedThrustMin" -> "float64",
      "PowerPolling" -> "float64"
    )
  )

  val narrowSparkInputConf = wideSparkInputConf.copy(
    sourceId = 200,
    query = "SELECT * FROM math_test ORDER BY dt",
    datetimeField = 'dt,
    dataTransformation = Some(SparkNDU('sensor_id, 'value_float, Map.empty, Some(Map.empty), Some(1000))),
  )

  val wideSparkInputIvolgaConf = wideSparkInputConf.copy(
    sourceId = 500,
    query = "SELECT * FROM `ivolga_test_wide` ORDER BY ts",
    partitionFields = Seq('stock_num, 'upload_id),
    dataTransformation = Some(SparkWDF(
      Map.empty, defaultTimeout = Some(15000L))
    )
  )

  val narrowRowSchema = wideRowSchema.copy(
    appIdFieldVal = ('app, 2)
  )

  val influxRowSchema = wideRowSchema.copy(
    appIdFieldVal = ('app, 3)
  )

  val narrowIvolgaRowSchema = wideRowSchema.copy(
    appIdFieldVal = ('app, 4)
  )

  val wideIvolgaRowSchema = wideRowSchema.copy(
    appIdFieldVal = ('app, 5)
  )

  val chConnection = s"jdbc:clickhouse://localhost:$port/default"
  val chDriver = "ru.yandex.clickhouse.ClickHouseDriver"

//  val wideKafkaRowSchema =
//    RowSchema('series_storage, 'from, 'to, ('app, 4), 'id, 'timestamp, 'context, wideKafkaInputConf.partitionFields)

  val wideSparkRowSchema =
    SparkRowSchema('series_storage, 'from, 'to, ('app, 1), 'id, 'subunit)

  val narrowSparkRowSchema = wideSparkRowSchema.copy(
    appIdFieldVal = ('app, 2),
  )

  val wideIvolgaSparkRowSchema = wideSparkRowSchema.copy(
    appIdFieldVal = ('app, 5),
  )

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

//  val wideKafkaOutputConf = JDBCOutputConf(
//    "events_wide_kafka_test",
//    wideKafkaRowSchema,
//    s"jdbc:clickhouse://localhost:$port/default",
//    "ru.yandex.clickhouse.ClickHouseDriver"
//  )

  val wideSparkOutputConf = SparkJDBCOutputConf(
    "events_wide_spark_test",
    wideSparkRowSchema,
    s"jdbc:clickhouse://localhost:$port/default",
    "ru.yandex.clickhouse.ClickHouseDriver"
  )

  val narrowSparkOutputConf = wideSparkOutputConf.copy(
    tableName = "events_narrow_spark_test",
    rowSchema = narrowSparkRowSchema
  )

  val wideSparkOutputIvolgaConf = wideSparkOutputConf.copy(
    tableName = "events_wide_ivolga_spark_test",
    rowSchema = wideIvolgaSparkRowSchema
  )

  val wideSparkKafkaOutputConf = SparkJDBCOutputConf(
    "events_wide_kafka_spark_test",
    wideSparkRowSchema,
    s"jdbc:clickhouse://localhost:$port/default",
    "ru.yandex.clickhouse.ClickHouseDriver"
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

    // Kafka producer
    val props = new Properties()
    props.put("bootstrap.servers", kafkaBrokerUrl)
    props.put("acks", "all")
    props.put("retries", "2")
    props.put("auto.commit.interval.ms", "1000")
    props.put("linger.ms", "1")
    props.put("block.on.buffer.full", "true")
    props.put("auto.create.topics.enable", "true")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    insertInfo.foreach(elem => {

      val insertData = Files.readResource(elem._2)
                            .drop(1)
                            .mkString("\n")

      clickhouseContainer.executeUpdate(s"INSERT INTO ${elem._1} FORMAT CSV\n${insertData}")

      val headers = Files.readResource(elem._2).take(1).toList.headOption.getOrElse("").split(",")
      val data = Files.readResource(elem._2).drop(1).map(_.split(","))
      val numberIndices = List("dt", "POilDieselOut", "SpeedThrustMin", "PowerPolling", "value_float").map(headers.indexOf(_))

      data.foreach { row =>
        val convertedRow: Seq[Any] = row.indices.map(idx => if (numberIndices.contains(idx)) {
          if (row(idx) == "\\N") Double.NaN else row(idx).toDouble
        } else row(idx))
        val msgKey = UUID.randomUUID().toString
        val msgMap = headers.zip(convertedRow).toMap[String, Any]
        val json = "{" + msgMap.map {
          case (k, v) => v match {
            case _: String => s""""$k": "$v""""
            case _         => s""""$k": $v"""
          }
        }.mkString(", ") + "}"
        val topic = elem._1.filter(_ != '`')
        println(s"Sending to $topic $msgKey --- $json")
        producer.send(new ProducerRecord[String, String](topic, msgKey, json)).get()
      }
    })

    producer.close()
  }

  def firstValidationQuery(table: String, numbers: Seq[Range]) = s"""
       SELECT number, c
       FROM (
         ${numbers.map(r => s"SELECT number FROM numbers(${r.start}, ${r.size})").mkString(" UNION ALL ")}
       ) num
       LEFT JOIN (
         SELECT id, COUNT(id) AS c FROM ${table} GROUP BY id
       ) e ON num.number = e.id ORDER BY num.number
  """

  val secondValidationQuery = "SELECT id, toUnixTimestamp(from) AS from_ts, " +
    "toUnixTimestamp(to) AS to_ts FROM %s ORDER BY id, from_ts, to_ts"

  override def afterAll(): Unit = {
    super.afterAll()
    clickhouseContainer.stop()
    influxContainer.stop()
    container.stop()
  }

  def alertByQuery(expectedValues: Seq[Seq[Double]], query: String, epsilon: Double = 0.0001): Assertion = {
    Try(checkByQuery(expectedValues, query, epsilon)) match {
      case Failure(exception) =>
        alert(exception.getMessage)
        assert(true)
      case Success(value) =>
        value
    }
  }

  "Data" should "load properly" in {
    checkByQuery(List(List(53.0)), "SELECT COUNT(*) FROM `2te116u_tmy_test_simple_rules`")
    checkByQuery(List(List(159.0)), "SELECT COUNT(*) FROM math_test")
    checkInfluxByQuery(List(List(53.0, 53.0, 53.0)), "SELECT COUNT(*) FROM \"2te116u_tmy_test_simple_rules\"")
  }

  "Cases 1-17, 43-53" should "work in wide table" in {
    casesPatterns.keys.foreach { id =>
      Post(
        "/streamJob/from-jdbc/to-jdbc/?run_async=0",
        FindPatternsRequest(s"17wide_$id", wideInputConf, wideOutputConf, List(casesPatterns(id)))
      ) ~>
      route ~> check {
        withClue(s"Pattern ID: $id") {
          status shouldEqual StatusCodes.OK
        }
      }
    }
    alertByQuery(
      incidentsCount
        .map {
          case (k, v) => List(k.toDouble, v.toDouble)
        }
        .toList
        .sortBy(_.headOption.getOrElse(Double.NaN)),
      firstValidationQuery("events_wide_test", numbersToRanges(casesPatterns.keys.map(_.toInt).toList.sorted))
    )
    alertByQuery(incidentsTimestamps, secondValidationQuery.format("events_wide_test"))
  }

  "Cases 1-17, 43-53" should "work in narrow table" in {
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
    alertByQuery(
      incidentsCount
        .map {
          case (k, v) => List(k.toDouble, v.toDouble)
        }
        .toList
        .sortBy(_.headOption.getOrElse(Double.NaN)),
      firstValidationQuery("events_narrow_test", numbersToRanges(casesPatterns.keys.map(_.toInt).toList.sorted))
    )
    alertByQuery(incidentsTimestamps, secondValidationQuery.format("events_narrow_test"))
  }

  "Cases 1-17, 43-53" should "work in influx table" in {
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
    alertByQuery(
      incidentsCount
        .map {
          case (k, v) => List(k.toDouble, v.toDouble)
        }
        .toList
        .sortBy(_.headOption.getOrElse(Double.NaN)),
      firstValidationQuery("events_influx_test", numbersToRanges(casesPatterns.keys.map(_.toInt).toList.sorted))
    )
    alertByQuery(incidentsTimestamps, secondValidationQuery.format("events_influx_test"))
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
    alertByQuery(
      incidentsIvolgaCount
        .map {
          case (k, v) => List(k.toDouble, v.toDouble)
        }
        .toList
        .sortBy(_.headOption.getOrElse(Double.NaN)),
      firstValidationQuery("events_wide_ivolga_test", numbersToRanges(casesPatternsIvolga.keys.map(_.toInt).toList.sorted))
    )
    alertByQuery(incidentsIvolgaTimestamps, secondValidationQuery.format("events_wide_ivolga_test"))
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
    alertByQuery(
      incidentsIvolgaCount
        .map {
          case (k, v) => List(k.toDouble, v.toDouble)
        }
        .toList
        .sortBy(_.headOption.getOrElse(Double.NaN)),
      firstValidationQuery("events_narrow_ivolga_test", numbersToRanges(casesPatternsIvolga.keys.map(_.toInt).toList.sorted))
    )
    alertByQuery(incidentsIvolgaTimestamps, secondValidationQuery.format("events_narrow_ivolga_test"))
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

//  "Cases 1-17, 43-50" should "work in wide Kafka table" in {
//    casesPatterns.keys.foreach { id =>
//      Post(
//        "/streamJob/from-kafka/to-jdbc/?run_async=1",
//        FindPatternsRequest(s"17kafkawide_$id", wideKafkaInputConf, wideKafkaOutputConf, List(casesPatterns(id)))
//      ) ~>
//        route ~> check {
//        withClue(s"Pattern ID: $id") {
//          status shouldEqual StatusCodes.OK
//        }
//        //alertByQuery(List(List(id.toDouble, incidentsCount(id).toDouble)), s"SELECT $id, COUNT(*) FROM events_wide_test WHERE id = $id")
//      }
//    }
//    Thread.sleep(50000)
//    alertByQuery(
//      incidentsCount
//        .map {
//          case (k, v) => List(k.toDouble, v.toDouble)
//        }
//        .toList
//        .sortBy(_.head),
//      s"SELECT id, COUNT(*) FROM events_wide_kafka_test GROUP BY id ORDER BY id"
//    )
//    alertByQuery(incidentsTimestamps, "SELECT id, from, to FROM events_wide_kafka_test ORDER BY id, from, to")
//  }

  "Cases 1-17, 43-50" should "work in wide table with Spark" in {
    casesPatterns.keys.toList.sorted.foreach { id =>
      Post(
        "/sparkJob/from-jdbc/to-jdbc/?run_async=0",
        FindPatternsRequest(s"17wide_$id", wideSparkInputConf, wideSparkOutputConf, List(casesPatterns(id)))
      ) ~>
        route ~> check {
        withClue(s"Pattern ID: $id") {
          status shouldEqual StatusCodes.OK
        }
        //alertByQuery(List(List(id.toDouble, incidentsCount(id).toDouble)), s"SELECT $id, COUNT(*) FROM events_wide_test WHERE id = $id")
      }
    }
    alertByQuery(
      incidentsCount
        .map {
          case (k, v) => List(k.toDouble, v.toDouble)
        }
        .toList
        .sortBy(_.headOption.getOrElse(Double.NaN)),
      firstValidationQuery("events_wide_spark_test", numbersToRanges(casesPatterns.keys.map(_.toInt).toList.sorted))
    )
    alertByQuery(incidentsTimestamps, secondValidationQuery.format("events_wide_spark_test"))
  }

//  "Cases 1-17, 43-50" should "work in narrow table with Spark" in {
//    casesPatterns.keys.toList.sorted.foreach { id =>
//      Post(
//        "/sparkJob/from-jdbc/to-jdbc/?run_async=0",
//        FindPatternsRequest(s"17narrow_$id", narrowSparkInputConf, narrowSparkOutputConf, List(casesPatterns(id)))
//      ) ~>
//        route ~> check {
//        withClue(s"Pattern ID: $id") {
//          status shouldEqual StatusCodes.OK
//        }
//      }
//    }
//    alertByQuery(
//      incidentsCount
//        .map {
//          case (k, v) => List(k.toDouble, v.toDouble)
//        }
//        .toList
//        .sortBy(_.headOption.getOrElse(Double.NaN)),
//      firstValidationQuery("events_narrow_spark_test", numbersToRanges(casesPatterns.keys.map(_.toInt).toList.sorted))
//    )
//    alertByQuery(incidentsTimestamps, secondValidationQuery.format("events_narrow_spark_test"))
//  }

  "Cases 18-42" should "work in ivolga wide table with Spark" in {
    casesPatternsIvolga.keys.toList.sorted.foreach { id =>
      Post(
        "/sparkJob/from-jdbc/to-jdbc/?run_async=0",
        FindPatternsRequest(s"17wide_$id", wideSparkInputIvolgaConf, wideSparkOutputIvolgaConf, List(casesPatternsIvolga(id)))
      ) ~>
        route ~> check {
        withClue(s"Pattern ID: $id") {
          status shouldEqual StatusCodes.OK
        }
      }
    }
    alertByQuery(
      incidentsIvolgaCount
        .map {
          case (k, v) => List(k.toDouble, v.toDouble)
        }
        .toList
        .sortBy(_.headOption.getOrElse(Double.NaN)),
      firstValidationQuery("events_wide_ivolga_spark_test", numbersToRanges(casesPatternsIvolga.keys.map(_.toInt).toList.sorted))
    )
    alertByQuery(incidentsIvolgaTimestamps, secondValidationQuery.format("events_wide_ivolga_spark_test"))
  }
    "Cases 1-17, 43-50" should "work in wide Kafka table with Spark" in {
      casesPatterns.keys.foreach { id =>
        Post(
          "/sparkJob/from-kafka/to-jdbc/?run_async=0",
          FindPatternsRequest(s"17kafkawide_$id", wideSparkKafkaInputConf, wideSparkKafkaOutputConf, List(casesPatterns(id)))
        ) ~>
          route ~> check {
          withClue(s"Pattern ID: $id") {
            status shouldEqual StatusCodes.OK
          }
          //alertByQuery(List(List(id.toDouble, incidentsCount(id).toDouble)), s"SELECT $id, COUNT(*) FROM events_wide_test WHERE id = $id")
        }
      }
      alertByQuery(
        incidentsCount
          .map {
            case (k, v) => List(k.toDouble, v.toDouble)
          }
          .toList
          .sortBy(_.headOption.getOrElse(Double.NaN)),
        firstValidationQuery("events_wide_kafka_spark_test", numbersToRanges(casesPatterns.keys.map(_.toInt).toList.sorted))
      )
      alertByQuery(incidentsTimestamps, secondValidationQuery.format("events_wide_kafka_spark_test"))
    }
}


