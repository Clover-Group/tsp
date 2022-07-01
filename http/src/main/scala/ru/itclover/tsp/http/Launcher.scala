package ru.itclover.tsp.http

import java.net.URLDecoder
import java.util.concurrent.{SynchronousQueue, ThreadPoolExecutor, TimeUnit}
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import cats.implicits._
import ru.itclover.tsp.RowWithIdx
import ru.itclover.tsp.http.routes.JobReporting
import ru.itclover.tsp.http.services.queuing.QueueManagerService
//import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor}
import scala.io.StdIn
import scala.util.Try

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

  override val reporting: Option[JobReporting] = {
    val reportingEnabledConfig = getEnvVarOrNone("JOB_REPORTING_ENABLED").getOrElse("0")
    val reportingEnabled = reportingEnabledConfig match {
      case "0" | "false" | "off" | "no" => false
      case "1" | "true" | "on" | "yes"  => true
      case _                            => sys.error(s"JOB_REPORTING_ENABLED not set or set to a unsupported value: $reportingEnabledConfig")
    }

    if (reportingEnabled) {
      val reportingBroker = getEnvVarOrNone("JOB_REPORTING_BROKER")
        .getOrElse(sys.error("Job reporting enabled, but JOB_REPORTING_BROKER not set"))
      val reportingTopic = getEnvVarOrNone("JOB_REPORTING_TOPIC")
        .getOrElse(sys.error("Job reporting enabled, but JOB_REPORTING_TOPIC not set"))
      log.warn(s"Job reporting enabled, sending to topic $reportingTopic on $reportingBroker")
      Some(JobReporting(reportingBroker, reportingTopic))
    } else {
      log.warn("Job reporting disabled")
      None
    }
  }

  implicit val rep = reporting
  val queueManager = QueueManagerService.getOrCreate(monitoringUri, blockingExecutorContext)
}
