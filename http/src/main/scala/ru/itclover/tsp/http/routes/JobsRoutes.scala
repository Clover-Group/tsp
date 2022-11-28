package ru.itclover.tsp.http.routes

import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCodes.PermanentRedirect
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.Materializer
import cats.data.Reader
import com.typesafe.scalalogging.Logger
import ru.itclover.tsp.StreamSource.Row
import ru.itclover.tsp._
import ru.itclover.tsp.core.io.AnyDecodersInstances
import ru.itclover.tsp.http.domain.input.FindPatternsRequest
import ru.itclover.tsp.http.protocols.RoutesProtocols
import ru.itclover.tsp.http.services.queuing.QueueManagerService
import spray.json._

import scala.concurrent.ExecutionContextExecutor

trait JobsRoutes extends RoutesProtocols {
  implicit val executionContext: ExecutionContextExecutor
  val blockingExecutionContext: ExecutionContextExecutor
  implicit val actorSystem: ActorSystem
  implicit val materializer: Materializer
  implicit val decoders: AnyDecodersInstances.type = AnyDecodersInstances

  val queueManager: QueueManagerService

  Logger[JobsRoutes]

  val route: Route =
    path("job" / "submit"./) {
      entity(as[FindPatternsRequest[RowWithIdx, String, Any, Row]]) { request =>
        queueManager.enqueue(request)
        complete(Map("status" -> s"Job ${request.uuid} enqueued.").toJson(propertyFormat))
      }
    } ~
    path("queue" / "show") {
      complete(
        queueManager.queueAsScalaSeq
          .map(_.asInstanceOf[FindPatternsRequest[RowWithIdx, String, Any, Row]])
          .toList
          .toJson
      )
    } ~
    path("queue" / Segment / "remove") { uuid =>
      queueManager.removeFromQueue(uuid) match {
        case Some(()) => complete(Map("status" -> s"Job $uuid removed from queue.").toJson(propertyFormat))
        case None     => redirect(s"/job/$uuid/stop", PermanentRedirect)
      }
    }
}

object JobsRoutes {

  private val log = Logger[JobsRoutes]

  def fromExecutionContext(blocking: ExecutionContextExecutor)(
    implicit as: ActorSystem,
    am: Materializer
  ): Reader[ExecutionContextExecutor, Route] = {

    log.debug("fromExecutionContext started")

    Reader { execContext =>
      new JobsRoutes {
        val blockingExecutionContext = blocking
        implicit val executionContext: ExecutionContextExecutor = execContext
        implicit val actorSystem = as
        implicit val materializer = am
        override val queueManager = QueueManagerService.getOrCreate("mgr", blocking)(
          execContext,
          as,
          am,
          AnyDecodersInstances
        )
      }.route
    }

  }

  log.debug("fromExecutionContext finished")
}
