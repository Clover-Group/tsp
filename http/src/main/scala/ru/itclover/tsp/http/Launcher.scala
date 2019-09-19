package ru.itclover.tsp.http

import java.net.URLDecoder

import java.util.concurrent.{SynchronousQueue, ThreadPoolExecutor, TimeUnit}

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import cats.implicits._
import cats.implicits._
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor}
import scala.io.StdIn

object Launcher extends App with HttpService {
  private val configs = ConfigFactory.load()
  override val isDebug: Boolean = configs.getBoolean("general.is-debug")
  private val log = Logger("Launcher")

  // bku: Increase the number of parallel connections
  val parallel = 1024

  // TSP-214 Fix
  val req_timeout = 1 // in mins

  implicit val system: ActorSystem = ActorSystem(
    "TSP-system",
    ConfigFactory.parseString(s"""
            |akka {
            |    http {
            |        server {
            |            request-timeout = $req_timeout min
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

  val streamEnvOrError = if (args.length > 0 && args(0) == "flink-cluster-test") {
    val (host, port) = getClusterHostPort match {
      case Right(hostAndPort) => hostAndPort
      case Left(err)          => throw new RuntimeException(err)
    }
    log.info(s"Starting TEST TSP on cluster Flink: $host:$port with monitoring in $monitoringUri")
    Right(StreamExecutionEnvironment.createRemoteEnvironment(host, port, args(1)))
  } else if (args.length != 1) {
    Left("You need to provide one arg: `flink-local` or `flink-cluster` to specify Flink execution mode.")
  } else if (args(0) == "flink-local") {
    createLocalEnv
  } else if (args(0) == "flink-cluster") {
    createClusterEnv
  } else {
    Left(s"Unknown argument: `${args(0)}`.")
  }

  implicit override val streamEnvironment = streamEnvOrError match {
    case Right(env) => env
    case Left(err)  => throw new RuntimeException(err)
  }

  streamEnvironment.setMaxParallelism(configs.getInt("flink.max-parallelism"))

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

  def createClusterEnv: Either[String, StreamExecutionEnvironment] = getClusterHostPort flatMap {
    case (clusterHost, clusterPort) =>
      log.info(s"Starting TSP on cluster Flink: $clusterHost:$clusterPort with monitoring in $monitoringUri")
      val rawJarPath = this.getClass.getProtectionDomain.getCodeSource.getLocation.getPath
      val jarPath = URLDecoder.decode(rawJarPath, "UTF-8")

      Either.cond(
        jarPath.endsWith(".jar"),
        StreamExecutionEnvironment.createRemoteEnvironment(clusterHost, clusterPort, jarPath),
        s"Jar path is invalid: `$jarPath` (no jar extension)"
      )
  }

  def createLocalEnv: Either[String, StreamExecutionEnvironment] = {
    log.info(s"Starting local Flink with monitoring in $monitoringUri")
    Right(StreamExecutionEnvironment.createLocalEnvironment())
  }
}
