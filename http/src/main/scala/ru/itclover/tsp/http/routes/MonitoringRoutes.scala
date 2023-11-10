package ru.itclover.tsp.http.routes

import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCodes.BadRequest
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.Materializer
import cats.data.Reader
import com.typesafe.scalalogging.Logger
import ru.itclover.tsp.BuildInfo
import ru.itclover.tsp.http.domain.input.FindPatternsRequest
import ru.itclover.tsp.http.domain.output.{FailureResponse, SuccessfulResponse}
import ru.itclover.tsp.http.protocols.RoutesProtocols
import ru.itclover.tsp.http.services.queuing.JobRunService
import ru.itclover.tsp.streaming.checkpointing.CheckpointingService
import ru.itclover.tsp.StreamSource.Row
import spray.json._

import scala.concurrent.ExecutionContextExecutor
import ru.itclover.tsp.RowWithIdx
import ru.itclover.tsp.http.services.coordinator.CoordinatorService

object MonitoringRoutes {

  private val log = Logger[MonitoringRoutes]

  def fromExecutionContext(
    jobRunService: JobRunService
  )(implicit as: ActorSystem, am: Materializer): Reader[ExecutionContextExecutor, Route] = {

    log.debug("fromExecutionContext started")

    Reader { execContext =>
      new MonitoringRoutes {
        implicit override val executionContext = execContext
        implicit override val actors = as
        implicit override val materializer = am
        implicit override val jrs = jobRunService
      }.route
    }

  }

  log.debug("fromExecutionContext finished")
}

trait MonitoringRoutes extends RoutesProtocols {
  implicit val jrs: JobRunService

  implicit val executionContext: ExecutionContextExecutor
  implicit val actors: ActorSystem
  implicit val materializer: Materializer

  val route: Route = path("job" / Segment / "status") { uuid =>
    CheckpointingService.getCheckpoint(uuid) match {
      case Some(details) =>
        complete(
          Map("rowsRead" -> details.readRows, "rowsWritten" -> details.totalWrittenRows)
            .toJson(implicitly[JsonWriter[Map[String, Any]]])
        )
      case None => complete((BadRequest, FailureResponse(4006, "No such job.", Seq.empty)))
      // case Failure(err)           => complete((InternalServerError, FailureResponse(5005, err)))
    }
  } ~ path("job" / Segment / "request") { uuid =>
    jrs.runningJobsRequests.get(uuid) match {
      case Some(r) =>
        complete(
          this
            .patternsRequestFmt[RowWithIdx, String, Any, Row]
            .write(r.asInstanceOf[FindPatternsRequest[RowWithIdx, String, Any, Row]])
        )
      case None => complete((BadRequest, FailureResponse(4006, "No such job.", Seq.empty)))
    }
  } ~ path("job" / Segment / "stop") { uuid =>
    jrs.getRunningJobsIds.find(_ == uuid) match {
      case Some(_) =>
        jrs.stopStream(uuid)
        complete(Map("message" -> s"Job $uuid stopped.").toJson(propertyFormat))
      case None =>
        complete((BadRequest, FailureResponse(4006, "No such job.", Seq.empty)))
    }
  } ~ path("jobs" / "overview") {
    complete(jrs.getRunningJobsIds.toJson)
  } ~
    path("metainfo" / "getVersion") {
      complete(
        SuccessfulResponse(
          Map(
            "id"    -> CoordinatorService.getTspId,
            "tsp"   -> BuildInfo.version,
            "scala" -> BuildInfo.scalaVersion
          )
        )
      )
    }

}
