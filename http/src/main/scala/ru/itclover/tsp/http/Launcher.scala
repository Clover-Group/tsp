package ru.itclover.tsp.http

import java.net.URLDecoder
import java.util.concurrent.{SynchronousQueue, ThreadPoolExecutor, TimeUnit}

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import cats.implicits._
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import ru.itclover.tsp.spark.StreamSource

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor}
import scala.io.StdIn

// We throw exceptions in the launcher.
// Also, some statements are non-Unit but we cannot use multiple `val _` in the same scope
@SuppressWarnings(Array("org.wartremover.warts.Throw", "org.wartremover.warts.NonUnitStatements"))
object Launcher extends App with HttpService {
  private val configs = ConfigFactory.load()
  override val isDebug: Boolean = configs.getBoolean("general.is-debug")
  private val log = Logger("Launcher")

  // bku: Increase the number of parallel connections
  val parallel = 1024

  // TSP-214 Fix
  val req_timeout = 120 // in mins

  // todo: no vars
  var useLocalSpark = true

  implicit val system: ActorSystem = ActorSystem(
    "TSP-system",
    ConfigFactory.parseString(s"""
            |akka {
            |    http {
            |        server {
            |            request-timeout = $req_timeout min
            |            idle-timeout = $req_timeout min
            |            backlog = $parallel
            |            pipelining-limit = $parallel
            |        }
            |
            |        host-connection-pool {
            |            max-connections = $parallel
            |            max-open-requests = $parallel
            |            max-connections = $parallel
            |        }
            |    }
            |}
          """.stripMargin)
  )

  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

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

  val streamEnvOrError = if (args.length != 1) {
    Left(
      "You need to provide one arg: `spark-xxx` where `xxx` can be `local` or `cluster` " +
      "to specify Spark execution mode."
    ) // TODO: More beautiful parsing
  } else if (args(0) == "spark-local") {
  } else if (args(0) == "spark-cluster") {
    useLocalSpark = false
  } else {
    Left(s"Unknown argument: `${args(0)}`.")
  }



  val spark = sparkSession

  private val host = configs.getString("http.host")
  private val port = configs.getInt("http.port")
  val bindingFuture = Http().bindAndHandle(route, host, port)

  log.info(s"Service online at http://$host:$port/" + (if (isDebug) " in debug mode." else ""))

  if (configs.getBoolean("general.is-follow-input")) {
    log.info("Press RETURN to stop...")
    StdIn.readLine()
    log.info("Terminating...")
    bindingFuture
      .flatMap(_.unbind())
      .onComplete(_ => Await.result(system.whenTerminated.map(_ => log.info("Terminated... Bye!")), 60.seconds))
  } else {
    scala.sys.addShutdownHook {
      log.info("Terminating...")
      system.terminate()
      Await.result(system.whenTerminated, 60.seconds)
      log.info("Terminated... Bye!")
    }
  }

  def getClusterHostPort: Either[String, (String, Int)] = {
    val host = getEnvVarOrConfig("FLINK_JOBMGR_HOST", "flink.job-manager.host")
    val portStr = getEnvVarOrConfig("FLINK_JOBMGR_PORT", "flink.job-manager.port")
    val port = Either.catchNonFatal(portStr.toInt).left.map { ex: Throwable =>
      s"Cannot parse FLINK_JOBMGR_PORT ($portStr): ${ex.getMessage}"
    }
    port.map(p => (host, p))
  }

  def getSparkHostPort: Either[String, (String, Int)] = {
    val host = getEnvVarOrConfig("SPARK_HOST", "spark.host")
    val portStr = getEnvVarOrConfig("SPARK_PORT", "spark.port")
    val port = Either.catchNonFatal(portStr.toInt).left.map { ex: Throwable =>
      s"Cannot parse SPARK_PORT ($portStr): ${ex.getMessage}"
    }
    port.map(p => (host, p))
  }

  def getSparkAddress: Either[String, String] = if (useLocalSpark) {
    Right("local")
  } else {
    getSparkHostPort.map { case (host, port) => s"spark://$host:$port" }
  }

  def sparkSession = getSparkAddress match {
    case Left(error) => throw new RuntimeException(error)
    case Right(address) =>
      StreamSource.sparkMaster = address
      if (address.startsWith("local")) {
        SparkSession
          .builder()
          .master(address)
          .appName("TSP Spark")
          .config("spark.io.compression.codec", "snappy")
          .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", true)
          .getOrCreate()
      } else {
        val host = getEnvVarOrConfig("SPARK_DRIVER", "spark.driver")
        SparkSession
          .builder()
          .master(address)
          .appName("TSP Spark")
          .config("spark.io.compression.codec", "snappy")
          .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", true)
          .config("spark.driver.host", host)
          .config("spark.driver.port", 2020)
          .config("spark.jars", "/opt/tsp.jar")
          .getOrCreate()
      }
  }
}
