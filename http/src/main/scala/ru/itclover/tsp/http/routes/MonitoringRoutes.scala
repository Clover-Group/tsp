package ru.itclover.tsp.http.routes

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.StatusCodes.{InternalServerError, BadRequest}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import akka.stream.ActorMaterializer
import com.typesafe.scalalogging.Logger
import ru.itclover.tsp.http.domain.output.{FailureResponse, SuccessfulResponse}
import ru.itclover.tsp.http.protocols.RoutesProtocols
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import cats.data.Reader
import ru.itclover.tsp.BuildInfo
import ru.itclover.tsp.http.services.flink.{MonitoringService, MonitoringServiceProtocols}
import ru.itclover.tsp.utils.Exceptions
import scala.util.{Failure, Success}

object MonitoringRoutes {
  def fromExecutionContext(monitoringUri: Uri)(implicit as: ActorSystem, am: ActorMaterializer): Reader[ExecutionContextExecutor, Route] =
    Reader { execContext =>
      new MonitoringRoutes {
        override implicit val executionContext = execContext
        override implicit val actors = as
        override implicit val materializer = am
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

  val noSuchJobWarn = "No such job or no connection to the FlinkMonitoring"

  private val log = Logger[MonitoringRoutes]

  val route: Route = path("job" / Segment / "status"./) { uuid =>
    onComplete(monitoring.queryJobInfo(uuid)) {
      case Success(Some(details)) => complete(details)
      case Success(None) => complete(BadRequest, FailureResponse(4006, "No such job.", Seq.empty))
      case Failure(err) => complete(InternalServerError, FailureResponse(5005, err))
    }
  } ~ path("job" / Segment / "exceptions"./) { uuid =>
    onComplete(monitoring.queryJobExceptions(uuid)) {
      case Success(Some(exceptions)) => complete(exceptions)
      case Success(None) => complete(BadRequest, FailureResponse(4006, "No such job.", Seq.empty))
      case Failure(err) => complete(InternalServerError, FailureResponse(5005, err))
    }
  } ~
  path("job" / Segment / "stop"./) { uuid =>
    onComplete(monitoring.sendStopQuery(uuid)) {
      case Success(Some(_)) => complete(SuccessfulResponse(1))
      case Success(None) => complete(BadRequest, FailureResponse(4006, "No such job.", Seq.empty))
      case Failure(err) => complete(InternalServerError, FailureResponse(5005, err))
    }
  } ~
  path("jobs" / "overview"./) {
    onComplete(monitoring.queryJobsOverview) {
      case Success(Some(resp)) => complete(resp)
      case Success(None) => complete(
        InternalServerError,
        FailureResponse(4006, "No connection to Flink Monitoring.", Seq.empty)
      )
      case Failure(err) => complete(InternalServerError, FailureResponse(5005, err))
    }
  } ~
  path("metainfo" /  "getVersion"./) {
    complete(SuccessfulResponse(BuildInfo.version))
  }
}
