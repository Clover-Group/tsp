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
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.types.Row
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
import ru.itclover.tsp.http.services.streaming.{FlinkMonitoringService, StatusReporter}
import ru.itclover.tsp.io.input.{InfluxDBInputConf, InputConf, JDBCInputConf, RedisInputConf}
import ru.itclover.tsp.io.output.{JDBCOutputConf, KafkaOutputConf, OutputConf}
import ru.itclover.tsp.mappers._

import scala.concurrent.{ExecutionContextExecutor, Future}
import ru.itclover.tsp.io.input.KafkaInputConf
import ru.itclover.tsp.utils.ErrorsADT.{ConfigErr, Err, GenericRuntimeErr, RuntimeErr}

case class JobReporting(brokers: String, topic: String)

trait JobsRoutes extends RoutesProtocols {
  implicit val executionContext: ExecutionContextExecutor
  val blockingExecutionContext: ExecutionContextExecutor
  implicit val streamEnv: StreamExecutionEnvironment
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
    /*path("queue" / "show") {
      complete(queueManager.queueAsScalaSeq.toList)
    } ~ */
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
    implicit strEnv: StreamExecutionEnvironment,
    as: ActorSystem,
    am: ActorMaterializer
  ): Reader[ExecutionContextExecutor, Route] = {

    log.debug("fromExecutionContext started")

    Reader { execContext =>
      new JobsRoutes {
        val blockingExecutionContext = blocking
        implicit val executionContext: ExecutionContextExecutor = execContext
        implicit val streamEnv: StreamExecutionEnvironment = strEnv
        implicit val actorSystem = as
        implicit val materializer = am
        override val monitoringUri = monitoringUrl
        override val queueManager = QueueManagerService.getOrCreate(monitoringUrl, blocking)(
          execContext, strEnv, as, am, AnyDecodersInstances, rep,
          implicitly[TypeInformation[RowWithIdx]], implicitly[TypeInformation[Row]]
        )
        override val reporting: Option[JobReporting] = rep
      }.route
    }

  }

  log.debug("fromExecutionContext finished")
}
