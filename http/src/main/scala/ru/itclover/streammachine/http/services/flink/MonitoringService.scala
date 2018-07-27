package ru.itclover.streammachine.http.services.flink

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json.{DefaultJsonProtocol, _}
import akka.http.scaladsl.model.{HttpEntity, _}
import akka.http.scaladsl.settings.{ClientConnectionSettings, ConnectionPoolSettings}
import akka.http.scaladsl.unmarshalling.{Unmarshal, Unmarshaller}
import akka.stream.{ActorMaterializer, Materializer}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import MonitoringServiceModel._
import ru.itclover.streammachine.utils.CollectionsOps.{OptionOps, RightBiasedEither}
import scala.io.StdIn
import scala.util.{Failure, Success}


case class MonitoringService(uri: Uri)(implicit as: ActorSystem, am: ActorMaterializer, ec: ExecutionContext)
     extends MonitoringServiceProtocols {

  val connectConf = ConnectionPoolSettings(as).withIdleTimeout(5.seconds).withResponseEntitySubscriptionTimeout(30.seconds)

  def queryJobInfo(name: String): Future[Option[JobDetails]] = queryJobByName(name) flatMap {
    case Some(job) => {
      val raw = Http().singleRequest(HttpRequest(uri = uri + s"/jobs/${job.jid}/"), settings=connectConf)
      val details = raw flatMap { rs => Unmarshal(rs.entity).to[Either[MonitoringError, JobDetails]] }
      details flatMap {
        case Right(r) => Future.successful(Some(r))
        case Left(err) => Future.failed(err.toThrowable)
      }
    }
    case None => Future.successful(None)
  }

  def sendStopQuery(name: String): Future[Option[Unit]] = queryJobByName(name) flatMap {
    case Some(job) =>
      val resp = for {
        response <- Http().singleRequest(
          HttpRequest(uri = uri + s"/jobs/${job.jid}", method = HttpMethods.PATCH),
          settings = connectConf
        )
        successOrErr <- Unmarshal(response.entity).to[Either[MonitoringError, EmptyResponse]]
      } yield successOrErr
      resp flatMap {
        case Right(_) => Future.successful(Some())
        case Left(err) => Future.failed(err.toThrowable)
      }
    case None => Future.successful(None)
  }

  def queryJobsOverview: Future[Option[JobsOverview]] = {
    val response = Http().singleRequest(HttpRequest(uri = uri + "/jobs/overview/"), settings = connectConf)
      .flatMap(resp => Unmarshal(resp.entity).to[Either[MonitoringError, JobsOverview]])
      .map(_.map(Some(_)))
      .recoverWith { // Special case - job finished and flink-monitoring unavailable
        case a: RuntimeException if a.getMessage.toLowerCase.contains("connection refused") =>
          Future.successful(Right(None))
        case x =>
          Future.failed(x)
      }
    response flatMap {
        case Right(r) => Future.successful(r)
        case Left(err) => Future.failed(err.toThrowable)
      }
  }

  /** Search job by name among all jobs. */
  def queryJobByName(name: String): Future[Option[JobInfo]] = queryJobsOverview map {
    case None => None
    case Some(overv) => overv.jobs.find(_.name == name)
  }
}

object MSTest extends App {
  implicit val system: ActorSystem = ActorSystem("Test")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  val ms = MonitoringService("http://localhost:8889")
  StdIn.readLine()

  println(Await.result(ms.sendStopQuery("qwe"), 10.seconds))
}

