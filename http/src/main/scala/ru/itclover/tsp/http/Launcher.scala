package ru.itclover.tsp.http

import java.util.concurrent.{SynchronousQueue, ThreadPoolExecutor, TimeUnit}
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import cats.implicits._
import ru.itclover.tsp.http.services.coordinator.CoordinatorService
import ru.itclover.tsp.http.services.queuing.QueueManagerService
import ru.itclover.tsp.streaming.checkpointing.CheckpointingService

//import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor}
import scala.io.StdIn

object Launcher extends App with HttpService {
  private val configs = ConfigFactory.load()
  override val isDebug: Boolean = configs.getBoolean("general.is-debug")
  private val log = Logger("Launcher")

  // bku: Increase the number of parallel connections
  val parallel = 1024

  // TSP-214 Fix
  val req_timeout = 120 // in mins

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
        new SynchronousQueue[Runnable]() //workQueue
        //new ThreadFactoryBuilder().setNameFormat("blocking-thread").setDaemon(true).build()
      )
    )

  private val host = configs.getString("http.host")
  private val port = configs.getInt("http.port")
  val bindingFuture = Http().bindAndHandle(route, host, port)

  log.info(s"Service online at http://$host:$port/" + (if (isDebug) " in debug mode." else ""))
  val coordinator = getCoordinatorHostPort
  coordinator.map {
    case (enabled, host, port) =>
      if (enabled) {
        val uri = s"http://$host:$port"
        log.warn(s"TSP coordinator connection enabled: connecting to $uri...")
        CoordinatorService.getOrCreate(uri).notifyRegister

      } else {
        log.warn("TSP coordinator connection disabled.")
      }
  }

  val checkpointing = getCheckpointingHostPort
  checkpointing.map {
    case (enabled, host, port) =>
      if (enabled) {
        val uri = s"redis://$host:$port"
        log.warn(s"TSP checkpointing enabled: registering service on $uri...")
        CheckpointingService.getOrCreate(Some(uri))
      } else {
        log.warn("TSP checkpointing disabled.")
        CheckpointingService.getOrCreate(None)
      }
  }

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

  def getCoordinatorHostPort: Either[String, (Boolean, String, Int)] = {
    val enabledStr = getEnvVarOrConfig("COORDINATOR_ENABLED", "coordinator.enabled")
    val host = getEnvVarOrConfig("COORDINATOR_HOST", "coordinator.host")
    val portStr = getEnvVarOrConfig("COORDINATOR_PORT", "coordinator.port")
    val port = Either.catchNonFatal(portStr.toInt).left.map { ex: Throwable =>
      s"Cannot parse COORDINATOR_PORT ($portStr): ${ex.getMessage}"
    }
    val enabled = Either.catchNonFatal(enabledStr.toBoolean) match {
      case Left(ex) => {
        log.warn(s"Cannot parse COORDINATOR_ENABLED ($enabledStr), defaulting to false:  ${ex.getMessage}")
        false
      }
      case Right(value) => value
    }
    port.map(p => (enabled, host, p))
  }

  def getCheckpointingHostPort: Either[String, (Boolean, String, Int)] = {
    val enabledStr = getEnvVarOrConfig("CHECKPOINTING_ENABLED", "checkpointing.enabled")
    val host = getEnvVarOrConfig("CHECKPOINTING_HOST", "checkpointing.host")
    val portStr = getEnvVarOrConfig("CHECKPOINTING_PORT", "checkpointing.port")
    val port = Either.catchNonFatal(portStr.toInt).left.map { ex: Throwable =>
      s"Cannot parse CHECKPOINTING_PORT ($portStr): ${ex.getMessage}"
    }
    val enabled = Either.catchNonFatal(enabledStr.toBoolean) match {
      case Left(ex) => {
        log.warn(s"Cannot parse CHECKPOINTING_ENABLED ($enabledStr), defaulting to false:  ${ex.getMessage}")
        false
      }
      case Right(value) => value
    }
    port.map(p => (enabled, host, p))
  }

  implicit val queueManagerService = QueueManagerService.getOrCreate("mgr", blockingExecutorContext)
}
