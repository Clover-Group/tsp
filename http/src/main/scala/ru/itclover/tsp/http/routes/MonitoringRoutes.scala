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
import ru.itclover.tsp.http.utils.ArchiveUtils
import akka.http.scaladsl.model.HttpEntity
import java.io.File
import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.server.StandardRoute
import scala.util.Try
import scala.util.Properties

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

  val logger = Logger[MonitoringRoutes]

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
  } ~ path("jobs" / Segment / "csvs") { uuid =>
    var r: StandardRoute = complete(
      (BadRequest, FailureResponse(4007, "CSV output not enabled, set CSV_OUTPUT_ENABLED to 1 to see CSVs", Seq.empty))
    )
    if (Properties.envOrElse("CSV_OUTPUT_ENABLED", "0") == "1") {
      val result = Try(ArchiveUtils.packCSV(uuid))
      if (result.isSuccess) {
        r = complete(HttpEntity.fromFile(MediaTypes.`application/zip`, new File(s"/tmp/sparse_intermediate/$uuid.zip")))
      } else {
        r = complete(
          (
            BadRequest,
            FailureResponse(4008, "Error occurred during archiving CSV outputs, job may not exist", Seq.empty)
          )
        )
      }
    }
    r
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
