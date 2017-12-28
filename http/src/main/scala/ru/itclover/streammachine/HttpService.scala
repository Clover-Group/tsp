package ru.itclover.streammachine

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Directives, ExceptionHandler, Route}
import akka.stream.ActorMaterializer
import ru.itclover.streammachine.domain.output.{FailureResponse, SuccessfulResponse}
import ru.itclover.streammachine.protocols.JsonProtocols
import ru.itclover.streammachine.routes.FindPatternRangesRoute

import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.concurrent.duration.Duration


trait HttpService extends Directives with JsonProtocols with FindPatternRangesRoute {
  val isDebug = false

  implicit val system: ActorSystem
  implicit val materializer: ActorMaterializer
  implicit val executionContext: ExecutionContextExecutor


  val defaultErrorsHandler: ExceptionHandler = ExceptionHandler {
    case ex: Exception =>
      val reason = if (isDebug) ex.getMessage else ""
      complete(FailureResponse("Internal server error", 500, reason))
  }

  override val route: Route =
    handleExceptions(defaultErrorsHandler) {
      path("streaming") {
        path("find-patterns") {
          get {
            val r = SuccessfulResponse(42)
            throw new Exception("Some reason")
            complete(r)
          }
        }
      }
    }
}
