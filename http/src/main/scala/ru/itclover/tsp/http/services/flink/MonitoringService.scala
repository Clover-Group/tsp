package ru.itclover.tsp.http.services.flink

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import scala.concurrent.{ExecutionContext, Future}
import MonitoringServiceModel._
import cats.implicits._
import cats._

case class MonitoringService(uri: Uri)(implicit as: ActorSystem, am: ActorMaterializer, ec: ExecutionContext)
    extends MonitoringServiceProtocols {

  import ru.itclover.tsp.http.services.AkkaHttpUtils._

  def queryJobDetailsWithMetrics(name: String, metricsInf: List[MetricInfo]): Future[Option[JobDetailsWithMetrics]] =
    queryJobInfo(name) flatMap {
      case Some(details) =>
        queryJobMetrics(details, metricsInf).map(metrics =>
          Some(JobDetailsWithMetrics(details, metrics.map({ case (inf, value) => inf.name -> value }).toMap))
        )
      case _ => Future.successful(None)
    }

  def queryJobInfo(name: String): Future[Option[JobDetails]] = queryJobByName(name) flatMap {
    case Some(job) => {
      val details = queryToEither[MonitoringError, JobDetails](uri + s"/jobs/${job.jid}/")
      details flatMap {
        case Right(r)  => Future.successful(Some(r))
        case Left(err) => Future.failed(err.toThrowable)
      }
    }
    case None => Future.successful(None)
  }

  def queryJobMetrics(job: JobDetails, metricsInfo: List[MetricInfo]): Future[List[(MetricInfo, String)]] = {
    Traverse[List].flatTraverse(metricsInfo)({ m =>
      val vertexInd = if (m.vertexIndex == Int.MaxValue) job.vertices.length - 1 else m.vertexIndex
      if (vertexInd < job.vertices.length) {
        val mUri = uri + s"/jobs/${job.jid}/vertices/${job.vertices(vertexInd).id}/metrics?get=${m.id}"
        queryToEither[MonitoringError, List[Metric]](mUri) flatMap {
          case Right(metrics) if metrics.nonEmpty => Future.successful(metrics.map(m -> _.value))
          case Right(_)                           => Future.successful(List(m -> "0"))

          case Left(err) => Future.failed(err.toThrowable)
        }
      } else {
        Future.failed(new IndexOutOfBoundsException("There is no such metric with index"))
      }
    })
  }

  def queryJobExceptions(name: String): Future[Option[JobExceptions]] = queryJobByName(name) flatMap {
    case Some(job) => {
      val raw = Http().singleRequest(HttpRequest(uri = uri + s"/jobs/${job.jid}/exceptions/"))
      val details = raw flatMap { rs =>
        Unmarshal(rs.entity).to[Either[MonitoringError, JobExceptions]]
      }
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
        response     <- Http().singleRequest(HttpRequest(uri = uri + s"/jobs/${job.jid}", method = HttpMethods.PATCH))
        successOrErr <- Unmarshal(response.entity).to[Either[MonitoringError, EmptyResponse]]
      } yield successOrErr
      resp flatMap {
        case Right(_)  => Future.successful(Some(Unit))
        case Left(err) => Future.failed(err.toThrowable)
      }
    case None => Future.successful(None)
  }

  def queryJobsOverview: Future[JobsOverview] = {
    val response = Http()
      .singleRequest(HttpRequest(uri = uri + "/jobs/overview/"))
      .flatMap(resp => Unmarshal(resp.entity).to[Either[MonitoringError, JobsOverview]])
    response flatMap {
      case Right(r)  => Future.successful(r)
      case Left(err) => Future.failed(err.toThrowable)
    }
  }

  /** Search job by name among all jobs. */
  def queryJobByName(name: String): Future[Option[JobBrief]] = queryJobsOverview.map(_.jobs.find(_.name == name))
}
