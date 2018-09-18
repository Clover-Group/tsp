package ru.itclover.tsp.http.services.flink

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.{ActorMaterializer, Materializer}
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import MonitoringServiceModel._
import ru.itclover.tsp.utils.CollectionsOps.{OptionOps, RightBiasedEither}


case class MonitoringService(uri: Uri)(implicit as: ActorSystem, am: ActorMaterializer, ec: ExecutionContext)
     extends MonitoringServiceProtocols {

  def queryJobInfo(name: String): Future[Option[JobDetails]] = queryJobByName(name) flatMap {
    case Some(job) => {
      val raw = Http().singleRequest(HttpRequest(uri = uri + s"/jobs/${job.jid}/"))
      val details = raw flatMap { rs => Unmarshal(rs.entity).to[Either[MonitoringError, JobDetails]] }
      details flatMap {
        case Right(r)  => Future.successful(Some(r))
        case Left(err) => Future.failed(err.toThrowable)
      }
    }
    case None => Future.successful(None)
  }

  def queryJobExceptions(name: String): Future[Option[JobExceptions]] = queryJobByName(name) flatMap {
    case Some(job) => {
      val raw = Http().singleRequest(HttpRequest(uri = uri + s"/jobs/${job.jid}/exceptions/"))
      val details = raw flatMap { rs => Unmarshal(rs.entity).to[Either[MonitoringError, JobExceptions]] }
      details flatMap {
        case Right(r)  => Future.successful(Some(r))
        case Left(err) => Future.failed(err.toThrowable)
      }
    }
    case None => Future.successful(None)
  }

  def sendStopQuery(jobName: String): Future[Option[Unit]] = queryJobByName(jobName) flatMap {
    case Some(job) =>
      val resp = for {
        response <- Http().singleRequest(HttpRequest(uri = uri + s"/jobs/${job.jid}", method = HttpMethods.PATCH))
        successOrErr <- Unmarshal(response.entity).to[Either[MonitoringError, EmptyResponse]]
      } yield successOrErr
      resp flatMap {
        case Right(_)  => Future.successful(Some())
        case Left(err) => Future.failed(err.toThrowable)
      }
    case None => Future.successful(None)
  }

  def queryJobsOverview: Future[Option[JobsOverview]] = {
    val response = Http().singleRequest(HttpRequest(uri = uri + "/jobs/overview/"))
      .flatMap(resp => Unmarshal(resp.entity).to[Either[MonitoringError, JobsOverview]])
      .map(_.map(Some(_)))
    // .. todo delete
      .recoverWith { // Special case - after job was finished - flink-monitoring became unavailable.
        case a: RuntimeException if a.getMessage.toLowerCase.contains("connection refused") =>
          Future.successful(Right(None))
        case x =>
          Future.failed(x)
      }
    response flatMap {
        case Right(r)  => Future.successful(r)
        case Left(err) => Future.failed(err.toThrowable)
      }
  }

  /** Search job by name among all jobs. */
  def queryJobByName(name: String): Future[Option[JobInfo]] = queryJobsOverview map {
    case None => None
    case Some(overv) => overv.jobs.find(_.name == name)
  }
}
