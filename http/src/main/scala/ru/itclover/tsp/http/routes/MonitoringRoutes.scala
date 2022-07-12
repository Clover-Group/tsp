package ru.itclover.tsp.http.routes

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes.{BadRequest, InternalServerError}
import akka.http.scaladsl.model.{HttpResponse, StatusCodes, Uri}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import cats.data.Reader
import com.typesafe.config.ConfigFactory
import fr.davit.akka.http.metrics.core.scaladsl.server.HttpMetricsSettings
import ru.itclover.tsp.BuildInfo
import ru.itclover.tsp.http.domain.output.{FailureResponse, SuccessfulResponse}
import ru.itclover.tsp.http.protocols.RoutesProtocols
import io.prometheus.client.CollectorRegistry
import fr.davit.akka.http.metrics.prometheus.PrometheusRegistry
import fr.davit.akka.http.metrics.prometheus.marshalling.PrometheusMarshallers._
import fr.davit.akka.http.metrics.core.scaladsl.server.HttpMetricsDirectives.metrics

import scala.concurrent.ExecutionContextExecutor
import com.typesafe.scalalogging.Logger
import ru.itclover.tsp.http.services.streaming.{FlinkMonitoringService, MonitoringServiceProtocols}
import spray.json.PrettyPrinter

import scala.util.{Failure, Success}

object MonitoringRoutes {

  private val log = Logger[MonitoringRoutes]

  def fromExecutionContext(
    monitoringUri: Uri
  )(implicit as: ActorSystem, am: ActorMaterializer): Reader[ExecutionContextExecutor, Route] = {

    log.debug("fromExecutionContext started")

    Reader { execContext =>
      new MonitoringRoutes {
        implicit override val executionContext = execContext
        implicit override val actors = as
        implicit override val materializer = am
        override val uri = monitoringUri
      }.route
    }

  }
  log.debug("fromExecutionContext finished")
}

trait MonitoringRoutes extends RoutesProtocols with MonitoringServiceProtocols {
  implicit val executionContext: ExecutionContextExecutor
  implicit val actors: ActorSystem
  implicit val materializer: ActorMaterializer

  val uri: Uri
  lazy val monitoring = FlinkMonitoringService(uri)
  implicit val printer = PrettyPrinter

  private val configs = ConfigFactory.load()

  val noSuchJobWarn = "No such job or no connection to the FlinkMonitoring"

  def checkResponse(elem: HttpResponse): Boolean =
    elem.status.isInstanceOf[StatusCodes.ServerError] &&
    elem.status.isInstanceOf[StatusCodes.ClientError]

  Logger[MonitoringRoutes]

  val akkaPrometheusRegistry = PrometheusRegistry(
    HttpMetricsSettings.default
      .withIncludeStatusDimension(true)
      .withIncludePathDimension(true)
      .withDefineError(checkResponse),
    new CollectorRegistry()
  )

  val route: Route = path("job" / Segment / "status") { uuid =>
      onComplete(monitoring.queryJobInfo(uuid)) {
        case Success(Some(details)) => complete((details))
        case Success(None)          => complete((BadRequest, FailureResponse(4006, "No such job.", Seq.empty)))
        case Failure(err)           => complete((InternalServerError, FailureResponse(5005, err)))
      }
    } ~
    path("job" / Segment / "exceptions") { uuid =>
      onComplete(monitoring.queryJobExceptions(uuid)) {
        case Success(Some(exceptions)) => complete((exceptions))
        case Success(None)             => complete((BadRequest, FailureResponse(4006, "No such job.", Seq.empty)))
        case Failure(err)              => complete((InternalServerError, FailureResponse(5005, err)))
      }
    } ~
    path("job" / Segment / "stop-flink") { uuid =>
      onComplete(monitoring.sendStopQuery(uuid)) {
        case Success(Some(_)) => complete((SuccessfulResponse(1)))
        case Success(None)    => complete((BadRequest, FailureResponse(4006, "No such job.", Seq.empty)))
        case Failure(err)     => complete((InternalServerError, FailureResponse(5005, err)))
      }
    } ~
    path("jobs" / "overview") {
      onComplete(monitoring.queryJobsOverview) {
        case Success(resp) => complete((resp))
        case Failure(err)  => complete((InternalServerError, FailureResponse(5005, err)))
      }
    } ~
    path("metainfo" / "getVersion") {
      complete(
        SuccessfulResponse(
          Map(
            "tsp"   -> BuildInfo.version,
            "scala" -> BuildInfo.scalaVersion,
            "flink" -> BuildInfo.flinkVersion
          )
        )
      )
    } ~
    (get & path("metrics-akka"))(metrics(akkaPrometheusRegistry))
}
