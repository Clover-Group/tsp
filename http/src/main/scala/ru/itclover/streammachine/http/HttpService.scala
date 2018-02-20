package ru.itclover.streammachine.http

import java.sql.Timestamp
import java.time.DateTimeException

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model.{HttpResponse, StatusCode, StatusCodes}
import akka.http.scaladsl.server._
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import cats.data.Reader
import ru.itclover.streammachine.http.domain.output.{FailureResponse, SuccessfulResponse}
import ru.itclover.streammachine.http.routes.FindPatternRangesRoute

import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import com.typesafe.scalalogging.Logger
import org.apache.flink.runtime.client.JobExecutionException
import ru.itclover.streammachine.http.protocols.JsonProtocols

import scala.io.StdIn


trait HttpService extends JsonProtocols {
  val isDebug = true
  implicit val system: ActorSystem
  implicit val materializer: ActorMaterializer
  implicit val executionContext: ExecutionContextExecutor
  implicit val streamEnvironment: StreamExecutionEnvironment

  private val log = Logger[HttpService]

  def composeRoutes: Reader[ExecutionContextExecutor, Route] = for {
    streams <- FindPatternRangesRoute.fromExecutionContext
    // ...
  } yield streams

  def route = handleExceptions(exceptionsHandler) {
    composeRoutes.run(executionContext)
  }


  def handleErrors: Directive[Unit] = handleRejections(rejectionsHandler) & handleExceptions(exceptionsHandler)

  def rejectionsHandler: RejectionHandler = RejectionHandler.newBuilder()
    .handleNotFound {
      extractUnmatchedPath { p =>
        complete(FailureResponse(1, s"Path not found: `$p`", Seq.empty))
      }
    }
    .handleAll[Rejection] { x =>
      complete(FailureResponse(StatusCodes.InternalServerError))
    }.result()

  def exceptionsHandler = ExceptionHandler {
    case ex: JobExecutionException =>
      complete(FailureResponse(2, s"Job execution failure", Seq(Option(ex.getCause).getOrElse(ex).getLocalizedMessage)))
    case ex: Exception => complete(FailureResponse(ex))
  }

}
