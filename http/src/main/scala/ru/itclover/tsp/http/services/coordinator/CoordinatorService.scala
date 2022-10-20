package ru.itclover.tsp.http.services.coordinator

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpMethods, HttpRequest, HttpResponse}
import com.typesafe.scalalogging.Logger
import ru.itclover.tsp.BuildInfo
import ru.itclover.tsp.streaming.checkpointing.CheckpointingService

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import java.util.concurrent.{ScheduledThreadPoolExecutor, ScheduledFuture, TimeUnit}


case class CoordinatorService(coordUri: String)(implicit as: ActorSystem, execCtx: ExecutionContext) {

  val log = Logger("CoordinatorService")

  val ex = new ScheduledThreadPoolExecutor(1)

  val task: Runnable = new Runnable {
    def run(): Unit = notifyRegister()
  }
  
  val f: ScheduledFuture[_] = ex.scheduleAtFixedRate(task, 0, 60, TimeUnit.SECONDS)

  def notifyRegister(): Unit = {
    val uri = s"$coordUri/api/tspinteraction/register"
    log.info(s"Notifying coordinator at $uri...")

    val responseFuture: Future[HttpResponse] = Http().singleRequest(
      HttpRequest(
        method = HttpMethods.POST,
        uri = uri,
        entity = HttpEntity(ContentTypes.`application/json`, s"""{"version": "${BuildInfo.version}"}""")
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
        entity = HttpEntity(ContentTypes.`application/json`, s"""{"jobId": "$jobId"}""")
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
    exception.map(_.getMessage).getOrElse("")

    val metrics = CheckpointingService.getCheckpoint(jobId)
    val (rowsRead, rowsWritten) = metrics.map(m => (m.readRows, m.writtenRows)).getOrElse((0, 0))

    val responseFuture: Future[HttpResponse] = Http().singleRequest(
      HttpRequest(
        method = HttpMethods.POST,
        uri = uri,
        entity = HttpEntity(
          ContentTypes.`application/json`,
          s"""
             |{"jobId": "$jobId",
             |"success": $success,
             |"error": "Exception occurred",
             |"rowsRead": $rowsRead,
             |"rowsWritten": $rowsWritten
             |}
             |""".stripMargin
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

  def getOrCreate(coordUri: String)(implicit as: ActorSystem, execCtx: ExecutionContext): CoordinatorService =
    service match {
      case Some(value) =>
        value
      case None =>
        val srv = CoordinatorService(coordUri)
        service = Some(srv)
        srv
    }

  def notifyRegister(): Unit = service.map(_.notifyRegister()).getOrElse(())

  def notifyJobStarted(jobId: String): Unit = service.map(_.notifyJobStarted(jobId)).getOrElse(())

  def notifyJobCompleted(jobId: String, exception: Option[Throwable]): Unit =
    service.map(_.notifyJobCompleted(jobId, exception)).getOrElse(())
}
