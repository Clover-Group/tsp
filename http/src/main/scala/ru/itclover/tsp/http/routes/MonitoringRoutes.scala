package ru.itclover.tsp.http.routes

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes.{BadRequest, InternalServerError}
import akka.http.scaladsl.model.{HttpResponse, StatusCodes, Uri}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import cats.data.Reader
import com.typesafe.scalalogging.Logger
import ru.itclover.tsp.BuildInfo
import ru.itclover.tsp.http.domain.output.{FailureResponse, SuccessfulResponse}
import ru.itclover.tsp.http.protocols.RoutesProtocols
import ru.itclover.tsp.http.services.queuing.QueueManagerService
import ru.itclover.tsp.streaming.checkpointing.CheckpointingService

import scala.concurrent.ExecutionContextExecutor
import scala.util.Success

object MonitoringRoutes {

  private val log = Logger[MonitoringRoutes]

  def fromExecutionContext(
                            queueManagerService: QueueManagerService
                          )(implicit as: ActorSystem, am: ActorMaterializer): Reader[ExecutionContextExecutor, Route] = {

    log.debug("fromExecutionContext started")

    Reader { execContext =>
      new MonitoringRoutes {
        implicit override val executionContext = execContext
        implicit override val actors = as
        implicit override val materializer = am
        implicit override val qm = queueManagerService
      }.route
    }

  }
  log.debug("fromExecutionContext finished")
}

trait MonitoringRoutes extends RoutesProtocols {
  implicit val qm: QueueManagerService

  implicit val executionContext: ExecutionContextExecutor
  implicit val actors: ActorSystem
  implicit val materializer: ActorMaterializer

  val route: Route = path("job" / Segment / "status") { uuid =>
    CheckpointingService.getCheckpoint(uuid) match {
      case Some(details) => complete(Map("rowsRead" -> details.readRows, "rowsWritten" -> details.writtenRows))
      case None          => complete((BadRequest, FailureResponse(4006, "No such job.", Seq.empty)))
      //case Failure(err)           => complete((InternalServerError, FailureResponse(5005, err)))
    }
  } ~ path("jobs" / "overview") {
    complete(qm.getRunningJobsIds)
  } ~
    path("metainfo" / "getVersion") {
      complete(
        SuccessfulResponse(
          Map(
            "tsp"   -> BuildInfo.version,
            "scala" -> BuildInfo.scalaVersion,
          )
        )
      )
    }

}
