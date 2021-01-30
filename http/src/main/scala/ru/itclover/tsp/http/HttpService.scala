package ru.itclover.tsp.http

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import akka.stream.ActorMaterializer
import cats.data.Reader
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import org.apache.flink.runtime.client.JobExecutionException
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.spark.sql.SparkSession
import ru.itclover.tsp.http.UtilsDirectives.{logRequest, logResponse}
import ru.itclover.tsp.http.domain.output.FailureResponse
import ru.itclover.tsp.http.protocols.RoutesProtocols
import ru.itclover.tsp.http.routes._
import ru.itclover.tsp.http.utils.Exceptions
import ru.itclover.tsp.http.utils.Exceptions.InvalidRequest
import ru.yandex.clickhouse.except.ClickHouseException

import scala.concurrent.ExecutionContextExecutor
import scala.util.Properties

trait HttpService extends RoutesProtocols {
  implicit val system: ActorSystem
  implicit val materializer: ActorMaterializer
  implicit val streamEnvironment: StreamExecutionEnvironment
  implicit val executionContext: ExecutionContextExecutor

  val blockingExecutorContext: ExecutionContextExecutor

  val spark: SparkSession

  private val configs = ConfigFactory.load()
  val isDebug = true
  val isHideExceptions = configs.getBoolean("general.is-hide-exceptions")
  val flinkMonitoringHost: String = getEnvVarOrConfig("FLINK_MONITORING_HOST", "flink.monitoring.host")
  val flinkMonitoringPort: Int = getEnvVarOrConfig("FLINK_MONITORING_PORT", "flink.monitoring.port").toInt
  val monitoringUri: Uri = s"http://$flinkMonitoringHost:$flinkMonitoringPort"

  private val log = Logger[HttpService]

  def composeRoutes: Reader[ExecutionContextExecutor, Route] = {
    log.debug("composeRoutes started")

    val res = for {
      jobs       <- JobsRoutes.fromExecutionContext(monitoringUri, blockingExecutorContext)
      monitoring <- MonitoringRoutes.fromExecutionContext(monitoringUri, spark)
      validation <- ValidationRoutes.fromExecutionContext()
    } yield jobs ~ monitoring ~ validation

    log.debug("composeRoutes finished")
    res
  }

  def route: Route = {
    log.debug("route started")
    val res = (logRequestAndResponse & handleErrors) {
      ignoreTrailingSlash {
        composeRoutes.run(executionContext).andThen { futureRoute =>
          futureRoute.onComplete { _ =>
            System.gc()
          } // perform full GC after each route
          futureRoute
        }
      }
    }
    log.debug("route finished")
    res
  }

  def logRequestAndResponse: Directive[Unit] = {
    log.debug("logRequestAndResponse started")
    val res = logRequest(log.info(_)) & logResponse(log.info(_))
    log.debug("logRequestAndResponse finished")
    res
  }

  def handleErrors: Directive[Unit] = {
    log.debug("handleErrors started")
    val res = handleRejections(rejectionsHandler) & handleExceptions(exceptionsHandler)
    log.debug("handleErrors finished")
    res
  }

  implicit def rejectionsHandler: RejectionHandler = RejectionHandler
    .newBuilder()
    .handleNotFound({
      extractUnmatchedPath { p =>
        complete((NotFound, FailureResponse(4004, s"Path not found: `$p`", Seq.empty)))
      }
    })
    .handleAll[MalformedFormFieldRejection] { x =>
      complete((BadRequest, FailureResponse(4001, "Malformed field.", x.map(_.toString))))
    }
    .handleAll[MalformedQueryParamRejection] { x =>
      complete((BadRequest, FailureResponse(4002, "Malformed query.", x.map(_.toString))))
    }
    .handleAll[MalformedRequestContentRejection] { x =>
      complete((BadRequest, FailureResponse(4003, "Malformed request content.", x.map(_.toString))))
    }
    .handleAll[Rejection] { _ =>
      complete((InternalServerError, FailureResponse(5003, "Unknown rejection.", Seq.empty)))
    }
    .result()

  implicit def exceptionsHandler = ExceptionHandler {
    case ex: ClickHouseException => // TODO Extract from jobs (ADT?)
      val stackTrace = Exceptions.getStackTrace(ex)
      val msg = if (ex.getCause != null) ex.getCause.getLocalizedMessage else ex.getMessage
      val error = s"Uncaught error during connection to Clickhouse, cause - `${msg}`, \n\nstacktrace: `$stackTrace`"
      log.error(error)
      complete(
        (
          InternalServerError,
          FailureResponse(5001, "Job execution failure", if (!isHideExceptions) Seq(error) else Seq.empty)
        )
      )

    case ex: JobExecutionException =>
      val stackTrace = Exceptions.getStackTrace(ex)
      val msg = if (ex.getCause != null) ex.getCause.getLocalizedMessage else ex.getMessage
      val error = s"Uncaught error during job execution, cause - `${msg}`, \n\nstacktrace: `$stackTrace`"
      log.error(error)
      complete(
        (
          InternalServerError,
          FailureResponse(5002, "Job execution failure", if (!isHideExceptions) Seq(error) else Seq.empty)
        )
      )

    case InvalidRequest(msg) =>
      log.error(msg)
      complete((BadRequest, FailureResponse(4005, "Invalid request", Seq(msg))))

    case ex @ (_: RuntimeException | _: java.io.IOException) =>
      val stackTrace = Exceptions.getStackTrace(ex)
      val msg = if (ex.getCause != null) ex.getCause.getLocalizedMessage else ex.getMessage
      val error = s"Uncaught error during request handling, cause - `${msg}`, \n\nstacktrace: `$stackTrace`"
      log.error(error)
      complete(
        (
          InternalServerError,
          FailureResponse(5005, "Request handling failure", if (!isHideExceptions) Seq(error) else Seq.empty)
        )
      )

    case ex: Exception =>
      val stackTrace = Exceptions.getStackTrace(ex)
      val msg = if (ex.getCause != null) ex.getCause.getLocalizedMessage else ex.getMessage
      val error = s"Uncaught error during request handling, cause - `${msg}`, \n\nstacktrace: `$stackTrace`"
      log.error(error)
      complete(
        (
          InternalServerError,
          FailureResponse(5008, "Request handling failure", if (!isHideExceptions) Seq(error) else Seq.empty)
        )
      )
  }

  def getEnvVarOrConfig(envVarName: String, configPath: String): String =
    Properties.envOrNone(envVarName).getOrElse(configs.getString(configPath))
}
