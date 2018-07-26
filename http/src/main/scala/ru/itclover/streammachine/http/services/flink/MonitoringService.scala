package ru.itclover.streammachine.http.services.flink

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json.{DefaultJsonProtocol, _}
import akka.http.scaladsl.model.{HttpEntity, _}
import akka.http.scaladsl.unmarshalling.{Unmarshal, Unmarshaller}
import akka.stream.{ActorMaterializer, Materializer}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import MonitoringServiceModel._
import ru.itclover.streammachine.utils.CollectionsOps.{OptionOps, RightBiasedEither}
import scala.io.StdIn


case class MonitoringService(uri: Uri)(implicit as: ActorSystem, am: ActorMaterializer, ec: ExecutionContext)
     extends MonitoringServiceProtocols {

  def queryJobInfo(name: String): Future[JobDetails] = queryJobIdByName(name) flatMap { jid =>
    val raw = Http().singleRequest(HttpRequest(uri=uri + s"/jobs/$jid/"))
    val details = raw flatMap { rs => Unmarshal(rs.entity).to[Either[MonitoringError, JobDetails]] }
    details flatMap {
      case Right(r) => Future.successful(r)
      case Left(err) => Future.failed(err.toThrowable)
    }
  }

  def queryJobIdByName(name: String): Future[String] = {
    // First query all jobs overview and unmarshall them
    val overview = for {
      response <- Http().singleRequest(HttpRequest(uri=uri + "/jobs/overview/"))
      overviewOrErr <- Unmarshal(response.entity).to[Either[MonitoringError, JobsOverview]]
    } yield {
      // Then search job by name among all jobs or return Left(err)
      overviewOrErr.flatMap { overview =>
        overview.jobs.find(_.name == name).toEither(MonitoringError(Seq(s"Cannot find job by name $name")))
      }
    }
    overview flatMap {
      case Right(r) => Future.successful(r.jid)
      case Left(err) => Future.failed(err.toThrowable)
    }
  }

  // TODO
  def sendStopQuery(jid: String): Future[Unit] = {
    Http().singleRequest(HttpRequest(uri=uri + s"/jobs/$jid", method=HttpMethods.PATCH)).map(_ => ())
  }

}

object MSTest extends App {
  implicit val system: ActorSystem = ActorSystem("Test")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  val ms = MonitoringService("http://localhost:8889")
  StdIn.readLine()

  println(Await.result(ms.queryJobIdByName("qwe"), 10.seconds))
  println(Await.result(ms.queryJobInfo("qwe"), 10.seconds))
}

