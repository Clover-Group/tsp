package ru.itclover.tsp.http.services.coordinator

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpMethods, HttpRequest, HttpResponse}
import com.typesafe.scalalogging.Logger
import ru.itclover.tsp.BuildInfo
import ru.itclover.tsp.streaming.checkpointing.CheckpointingService
import spray.json._
import spray.json.DefaultJsonProtocol._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import java.util.concurrent.{ScheduledThreadPoolExecutor, ScheduledFuture, TimeUnit}

case class CoordinatorService(
  coordUri: String,
  advHost: Option[String],
  advPort: Option[Int]
)(implicit as: ActorSystem, execCtx: ExecutionContext) {

  case class RegisterMessage(
    version: String,
    uuid: String,
    advertisedIP: Option[String],
    advertisedPort: Option[Int]
  )

  case class JobStartedMessage(jobId: String)

  case class JobCompletedMessage(
    jobId: String,
    success: Boolean,
    error: String,
    rowsRead: Long,
    rowsWritten: Long
  )

  object MessageJsonProtocol extends DefaultJsonProtocol {
    implicit val versionMessageFormat: RootJsonFormat[RegisterMessage] = jsonFormat4(RegisterMessage.apply)
    implicit val jobStartedMessageFormat: RootJsonFormat[JobStartedMessage] = jsonFormat1(JobStartedMessage.apply)
    implicit val jobCompletedMessageFormat: RootJsonFormat[JobCompletedMessage] = jsonFormat5(JobCompletedMessage.apply)
  }

  import MessageJsonProtocol._

  val log = Logger("CoordinatorService")

  val ex = new ScheduledThreadPoolExecutor(1)

  val uuid = java.util.UUID.randomUUID.toString

  val task: Runnable = new Runnable {
    def run(): Unit = notifyRegister()
  }

  val f: ScheduledFuture[_] = ex.scheduleAtFixedRate(task, 30, 60, TimeUnit.SECONDS)

  def notifyRegister(): Unit = {

    val uri = s"$coordUri/api/tspinteraction/register"
    log.info(s"Notifying coordinator at $uri...")

    val responseFuture: Future[HttpResponse] = Http().singleRequest(
      HttpRequest(
        method = HttpMethods.POST,
        uri = uri,
        entity = HttpEntity(
          ContentTypes.`application/json`,
          RegisterMessage(
            BuildInfo.version,
            uuid,
            advHost,
            advPort
          ).toJson.compactPrint
        )
      )
    )

    responseFuture
      .onComplete {
        case Success(res) => {
          if (res.status.isFailure) log.error(s"Error: TSP coordinator returned ${res.status}: ${res.entity.toString}")
        }
        case Failure(ex) => log.error(s"Cannot connect to $uri: $ex")
      }
  }

  def notifyJobStarted(jobId: String): Unit = {
    val uri = s"$coordUri/api/tspinteraction/jobstarted"
    log.warn(s"Job $jobId started: notifying coordinator to $uri...")

    val responseFuture: Future[HttpResponse] = Http().singleRequest(
      HttpRequest(
        method = HttpMethods.POST,
        uri = uri,
        entity = HttpEntity(ContentTypes.`application/json`, JobStartedMessage(jobId).toJson.compactPrint)
      )
    )

    responseFuture
      .onComplete {
        case Success(res) => {
          if (res.status.isFailure) log.error(s"Error: TSP coordinator returned ${res.status}: ${res.entity.toString}")
        }
        case Failure(ex) => log.error(s"Cannot connect to $uri: $ex")
      }
  }

  def notifyJobCompleted(jobId: String, exception: Option[Throwable]): Unit = {

    val uri = s"$coordUri/api/tspinteraction/jobcompleted"
    log.warn(s"Job $jobId completed: notifying coordinator to $uri...")

    val success = exception.isEmpty
    val error = exception.map(_.getMessage).getOrElse("")

    val metrics = CheckpointingService.getCheckpoint(jobId)
    val (rowsRead, rowsWritten) = metrics.map(m => (m.readRows, m.totalWrittenRows)).getOrElse((0L, 0L))

    val responseFuture: Future[HttpResponse] = Http().singleRequest(
      HttpRequest(
        method = HttpMethods.POST,
        uri = uri,
        entity = HttpEntity(
          ContentTypes.`application/json`,
          JobCompletedMessage(jobId, success, error, rowsRead, rowsWritten).toJson.compactPrint
        )
      )
    )

    responseFuture
      .onComplete {
        case Success(res) => {
          if (res.status.isFailure) log.error(s"Error: TSP coordinator returned ${res.status}: ${res.entity.toString}")
        }
        case Failure(ex) => log.error(s"Cannot connect to $uri: $ex")
      }
  }

}

object CoordinatorService {
  private var service: Option[CoordinatorService] = None

  def getOrCreate(coordUri: String, advHost: Option[String], advPort: Option[Int])(implicit
    as: ActorSystem,
    execCtx: ExecutionContext
  ): CoordinatorService =
    service match {
      case Some(value) =>
        value
      case None =>
        val srv = CoordinatorService(coordUri, advHost, advPort)
        service = Some(srv)
        srv
    }

  def getTspId: String = service.map(_.uuid).getOrElse("")

  def notifyRegister(): Unit = service.map(_.notifyRegister()).getOrElse(())

  def notifyJobStarted(jobId: String): Unit = service.map(_.notifyJobStarted(jobId)).getOrElse(())

  def notifyJobCompleted(jobId: String, exception: Option[Throwable]): Unit =
    service.map(_.notifyJobCompleted(jobId, exception)).getOrElse(())

}
