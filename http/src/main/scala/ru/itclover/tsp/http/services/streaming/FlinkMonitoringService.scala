package ru.itclover.tsp.http.services.streaming

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import cats._
import cats.implicits._
import ru.itclover.tsp.http.services.streaming.MonitoringServiceModel._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

case class FlinkMonitoringService(uri: Uri)(implicit as: ActorSystem, am: ActorMaterializer, ec: ExecutionContext)
    extends MonitoringServiceProtocols {

  import ru.itclover.tsp.http.services.AkkaHttpUtils._

  def queryJobInfo(name: String): Future[Option[JobDetails]] = queryJobByName(name).flatMap {
    case Some(job) => {
      val details = queryToEither[MonitoringError, JobDetails](uri + s"/jobs/${job.jid}/")
      details.flatMap {
        case Right(r)  => Future.successful(Some(r))
        case Left(err) => Future.failed(err.toThrowable)
      }
    }
    case None => Future.successful(None)
  }

  def queryJobExceptions(name: String): Future[Option[JobExceptions]] = queryJobByName(name).flatMap {
    case Some(job) => {
      val raw = Http().singleRequest(HttpRequest(uri = uri + s"/jobs/${job.jid}/exceptions/"))
      val details = raw.flatMap { rs =>
        Unmarshal(rs.entity).to[Either[MonitoringError, JobExceptions]]
      }
      details.flatMap {
        case Right(r)  => Future.successful(Some(r))
        case Left(err) => Future.failed(err.toThrowable)
      }
    }
    case None => Future.successful(None)
  }

  def sendStopQuery(jobName: String): Future[Option[Unit]] = queryJobByName(jobName).flatMap {
    case Some(job) =>
      val resp = for {
        response     <- Http().singleRequest(HttpRequest(uri = uri + s"/jobs/${job.jid}", method = HttpMethods.PATCH))
        successOrErr <- Unmarshal(response.entity).to[Either[MonitoringError, EmptyResponse]]
      } yield successOrErr
      resp.flatMap {
        case Right(_)  => Future.successful(Some(()))
        case Left(err) => Future.failed(err.toThrowable)
      }
    case None => Future.successful(None)
  }

  def queryJobsOverview: Future[JobsOverview] = {
    val response = Http()
      .singleRequest(HttpRequest(uri = uri + "/jobs/overview/"))
      .flatMap(resp => Unmarshal(resp.entity).to[Either[MonitoringError, JobsOverview]])
    response.flatMap {
      case Right(r)  => Future.successful(r)
      case Left(err) => Future.failed(err.toThrowable)
    }
  }

  /** Search job by name among all jobs. */
  def queryJobByName(name: String): Future[Option[JobBrief]] = queryJobsOverview.map(_.jobs.find(_.name == name))
}
