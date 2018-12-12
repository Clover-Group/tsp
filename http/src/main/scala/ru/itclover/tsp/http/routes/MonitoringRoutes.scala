package ru.itclover.tsp.http.routes

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.StatusCodes.{BadRequest, InternalServerError}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import akka.stream.ActorMaterializer
import com.typesafe.scalalogging.Logger
import ru.itclover.tsp.http.domain.output.{FailureResponse, SuccessfulResponse}
import ru.itclover.tsp.http.protocols.RoutesProtocols

import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import cats.data.Reader
import com.typesafe.config.ConfigFactory
import ru.itclover.tsp.BuildInfo
// import ru.itclover.tsp.BuildInfo
import ru.itclover.tsp.http.services.flink.{MonitoringService, MonitoringServiceProtocols}
import ru.itclover.tsp.http.services.flink.MonitoringServiceModel.MetricInfo
import ru.itclover.tsp.utils.Exceptions
import spray.json.PrettyPrinter
import scala.util.{Failure, Success}

object MonitoringRoutes {

  def fromExecutionContext(
    monitoringUri: Uri
  )(implicit as: ActorSystem, am: ActorMaterializer): Reader[ExecutionContextExecutor, Route] =
    Reader { execContext =>
      new MonitoringRoutes {
        implicit override val executionContext = execContext
        implicit override val actors = as
        implicit override val materializer = am
        override val uri = monitoringUri
      }.route
    }
}

trait MonitoringRoutes extends RoutesProtocols with MonitoringServiceProtocols {
  implicit val executionContext: ExecutionContextExecutor
  implicit val actors: ActorSystem
  implicit val materializer: ActorMaterializer

  val uri: Uri
  lazy val monitoring = MonitoringService(uri)
  implicit val printer = PrettyPrinter

  private val configs = ConfigFactory.load()

  val metricsInfo = List(
    MetricInfo(0, configs.getString("flink.metrics.source"), "numRecordsRead"),
    // MetricInfo(1, configs.getString("flink.metrics.search"), PatternMapper.currentEventTsMetric), // todo
    MetricInfo.onLastVertex(configs.getString("flink.metrics.sink"), "numRecordsProcessed")
  )

  val noSuchJobWarn = "No such job or no connection to the FlinkMonitoring"

  private val log = Logger[MonitoringRoutes]

  val route: Route = path("job" / Segment / "statusAndMetrics") { uuid =>
    onComplete(monitoring.queryJobDetailsWithMetrics(uuid, metricsInfo)) {
      case Success(Some(detailsAndMetrics)) => complete(SuccessfulResponse(detailsAndMetrics))
      case Success(None)                    => complete(BadRequest, FailureResponse(4006, "No such job.", Seq.empty))
      case Failure(err)                     => complete(InternalServerError, FailureResponse(5005, err))
    }
  } ~
  path("job" / Segment / "status") { uuid =>
    onComplete(monitoring.queryJobInfo(uuid)) {
      case Success(Some(details)) => complete(details)
      case Success(None)          => complete(BadRequest, FailureResponse(4006, "No such job.", Seq.empty))
      case Failure(err)           => complete(InternalServerError, FailureResponse(5005, err))
    }
  } ~
  path("job" / Segment / "exceptions") { uuid =>
    onComplete(monitoring.queryJobExceptions(uuid)) {
      case Success(Some(exceptions)) => complete(exceptions)
      case Success(None)             => complete(BadRequest, FailureResponse(4006, "No such job.", Seq.empty))
      case Failure(err)              => complete(InternalServerError, FailureResponse(5005, err))
    }
  } ~
  path("job" / Segment / "stop") { uuid =>
    onComplete(monitoring.sendStopQuery(uuid)) {
      case Success(Some(_)) => complete(SuccessfulResponse(1))
      case Success(None)    => complete(BadRequest, FailureResponse(4006, "No such job.", Seq.empty))
      case Failure(err)     => complete(InternalServerError, FailureResponse(5005, err))
    }
  } ~
  path("jobs" / "overview") {
    onComplete(monitoring.queryJobsOverview) {
      case Success(resp) => complete(resp)
      case Failure(err)  => complete(InternalServerError, FailureResponse(5005, err))
    }
  } ~
  path("metainfo" / "getVersion") {
    complete(SuccessfulResponse(BuildInfo.version))
  }
}
