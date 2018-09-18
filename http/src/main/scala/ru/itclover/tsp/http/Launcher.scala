package ru.itclover.tsp.http

import scala.util.{Failure, Properties, Success, Try}
import java.net.URLDecoder
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.concurrent.duration._
import scala.io.StdIn
import cats.implicits._

object Launcher extends App with HttpService {
  private val configs = ConfigFactory.load()
  override val isDebug: Boolean = configs.getBoolean("general.is-debug")
  private val log = Logger("Launcher")

  implicit val system: ActorSystem = ActorSystem("TSP-system")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  val streamEnvOrError = if (args.length != 1) {
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

  def createClusterEnv: Either[String, StreamExecutionEnvironment] = for {
    clusterPort <- Properties
      .propOrNone("FLINK_JOBMGR_PORT")
      .map(p =>
        Either.catchNonFatal(p.toInt).left.map { ex: Throwable =>
          s"Cannot parse FLINK_JOBMGR_PORT ($p): ${ex.getMessage}"
        }
      )
      .getOrElse(Right(configs.getInt("flink.job-manager.port")))
    clusterHost = Properties.envOrElse("FLINK_JOBMGR_HOST", configs.getString("flink.job-manager.host"))

    rawJarPath = this.getClass.getProtectionDomain.getCodeSource.getLocation.getPath
    jarPath = URLDecoder.decode(rawJarPath, "UTF-8")
    _ <- Either.cond(
      jarPath.endsWith(".jar"),
      Unit,
      s"Jar path is invalid: `$jarPath` (no jar extension)"
    )
  } yield {
    log.info(s"Starting TSP on cluster Flink: $clusterHost:$clusterPort")
    StreamExecutionEnvironment.createRemoteEnvironment(clusterHost, clusterPort, jarPath)
  }

  def createLocalEnv: Either[String, StreamExecutionEnvironment] = {
    log.info("Starting local Flink")
    Right(StreamExecutionEnvironment.createLocalEnvironment())
  }
}
