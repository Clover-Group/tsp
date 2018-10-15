package ru.itclover.tsp.http.routes
import akka.actor.ActorSystem
import akka.http.scaladsl.marshalling
import akka.http.scaladsl.marshalling.{Marshaller, Marshalling, ToResponseMarshallable, ToResponseMarshaller}
import akka.http.scaladsl.model.{HttpResponse, Uri}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.util.FastFuture
import akka.stream.ActorMaterializer
import cats.data.Reader
import ru.itclover.tsp.core.Time
import ru.itclover.tsp.core.Time.TimeExtractor
import ru.itclover.tsp.dsl.schema.RawPattern
import ru.itclover.tsp.dsl.{PatternsValidator, PatternsValidatorConf}
import ru.itclover.tsp.http.protocols.{PatternsValidatorProtocols, RoutesProtocols, ValidationResult}
import ru.itclover.tsp.phases.NumericPhases.SymbolNumberExtractor

import scala.concurrent.ExecutionContextExecutor

object ValidationRoutes {

  def fromExecutionContext(
    monitoringUri: Uri
  )(implicit as: ActorSystem, am: ActorMaterializer): Reader[ExecutionContextExecutor, Route] =
    Reader { execContext =>
      new ValidationRoutes {
      }.route
    }
}

trait ValidationRoutes extends RoutesProtocols with PatternsValidatorProtocols {

  val route: Route = path("patterns" / "validate"./) {
    entity(as[PatternsValidatorConf]) { request =>
      val patterns: Seq[RawPattern] = request.patterns
      val res = PatternsValidator.validate[Nothing](patterns)(
        new TimeExtractor[Nothing] { override def apply(v1: Nothing): Time = Time(0) },
        new SymbolNumberExtractor[Nothing] { override def extract(event: Nothing, symbol: Symbol): Double = 0.0 }
      )
      val result = res.map { x =>
        x._2 match {
          case Right(success) =>
            ValidationResult(pattern = x._1, success = true, context = success.toString)
          case Left(error) =>
            ValidationResult(pattern = x._1, success = false, context = error.toString)
        }
      }
      complete(result)
    }
  }
}
