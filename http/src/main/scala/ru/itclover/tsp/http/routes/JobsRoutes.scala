package ru.itclover.tsp.http.routes

import java.util.concurrent.TimeUnit
import akka.actor.ActorSystem
import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model.StatusCodes.{BadRequest, InternalServerError, NotFound, PermanentRedirect}
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import cats.data.Reader
import cats.implicits._
import com.typesafe.scalalogging.Logger
import ru.itclover.tsp.StreamSource.Row
import ru.itclover.tsp._
import ru.itclover.tsp.core.RawPattern
import ru.itclover.tsp.core.io.{AnyDecodersInstances, BasicDecoders}
import ru.itclover.tsp.http.domain.input.FindPatternsRequest
import ru.itclover.tsp.core.io.{AnyDecodersInstances, BasicDecoders}
import ru.itclover.tsp.dsl.PatternFieldExtractor
import ru.itclover.tsp.http.domain.input.FindPatternsRequest
import ru.itclover.tsp.http.domain.output.SuccessfulResponse.ExecInfo
import ru.itclover.tsp.http.domain.output._
import ru.itclover.tsp.http.protocols.RoutesProtocols
import ru.itclover.tsp.http.services.queuing.QueueManagerService
import ru.itclover.tsp.http.services.streaming.FlinkMonitoringService
import ru.itclover.tsp.streaming.io.{InputConf, JDBCInputConf, KafkaInputConf}
import ru.itclover.tsp.streaming.io.{JDBCOutputConf, KafkaOutputConf, OutputConf}
import ru.itclover.tsp.streaming.mappers._

import scala.concurrent.{ExecutionContextExecutor, Future}
import ru.itclover.tsp.streaming.utils.ErrorsADT.{ConfigErr, Err, GenericRuntimeErr, RuntimeErr}

case class JobReporting(brokers: String, topic: String)

trait JobsRoutes extends RoutesProtocols {
  implicit val executionContext: ExecutionContextExecutor
  val blockingExecutionContext: ExecutionContextExecutor
  implicit val actorSystem: ActorSystem
  implicit val materializer: ActorMaterializer
  implicit val decoders = AnyDecodersInstances

  val monitoringUri: Uri
  lazy val monitoring = FlinkMonitoringService(monitoringUri)
  val queueManager: QueueManagerService

  implicit val reporting: Option[JobReporting]

  private val log = Logger[JobsRoutes]

  val route: Route =
    path("job" / "submit"./) {
      entity(as[FindPatternsRequest[RowWithIdx, Symbol, Any, Row]]) { request =>
        queueManager.enqueue(request)
        complete(Map("status" -> s"Job ${request.uuid} enqueued."))
      }
    } ~
    path("queue" / "show") {
      complete(queueManager.queueAsScalaSeq
        .map(_.asInstanceOf[FindPatternsRequest[RowWithIdx, Symbol, Any, Row]])
        .toList)
    } ~
    path("queue" / Segment / "remove") { uuid =>
      queueManager.removeFromQueue(uuid) match {
        case Some(()) => complete(Map("status" -> s"Job $uuid removed from queue."))
        case None => redirect(s"/job/$uuid/stop", PermanentRedirect)
      }
    }
}

object JobsRoutes {

  private val log = Logger[JobsRoutes]

  def fromExecutionContext(monitoringUrl: Uri,
                           rep: Option[JobReporting],
                           blocking: ExecutionContextExecutor)(
    implicit as: ActorSystem,
    am: ActorMaterializer
  ): Reader[ExecutionContextExecutor, Route] = {

    log.debug("fromExecutionContext started")

    Reader { execContext =>
      new JobsRoutes {
        val blockingExecutionContext = blocking
        implicit val executionContext: ExecutionContextExecutor = execContext
        implicit val actorSystem = as
        implicit val materializer = am
        override val monitoringUri = monitoringUrl
        override val queueManager = QueueManagerService.getOrCreate(monitoringUrl, blocking)(
          execContext, as, am, AnyDecodersInstances, rep
        )
        override val reporting: Option[JobReporting] = rep
      }.route
    }

  }

  log.debug("fromExecutionContext finished")
}
