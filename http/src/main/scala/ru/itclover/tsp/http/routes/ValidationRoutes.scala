package ru.itclover.tsp.http.routes
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import cats.data.Reader
import ru.itclover.tsp.core.RawPattern
import ru.itclover.tsp.dsl.{PatternsValidator, PatternsValidatorConf}
import ru.itclover.tsp.http.protocols.{PatternsValidatorProtocols, RoutesProtocols, ValidationResult}
import spray.json._

import scala.concurrent.ExecutionContextExecutor

object ValidationRoutes {

  def fromExecutionContext(): Reader[ExecutionContextExecutor, Route] =
    Reader(_ => new ValidationRoutes {}.route)
}

trait ValidationRoutes extends RoutesProtocols with PatternsValidatorProtocols {

  val route: Route = path("patterns" / "validate"./) {
    entity(as[PatternsValidatorConf]) { request =>
      val patterns: Seq[RawPattern] = request.patterns
      val fields: Map[String, String] = request.fields
      val res = PatternsValidator.validate[Nothing](patterns, fields)()
      val result = res.map { x =>
        x._2 match {
          case Right(success) =>
            ValidationResult(pattern = x._1, success = true, context = success.toString)
          case Left(error) =>
            ValidationResult(pattern = x._1, success = false, context = error.toString)
        }
      }
      complete(result.toJson)
    }
  }
}
