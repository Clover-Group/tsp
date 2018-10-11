package ru.itclover.tsp.http.routes
import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import cats.data.Reader
import ru.itclover.tsp.core.Time
import ru.itclover.tsp.core.Time.TimeExtractor
import ru.itclover.tsp.dsl.{PatternsValidator, PatternsValidatorConf}
import ru.itclover.tsp.http.protocols.{RoutesProtocols, PatternsValidatorProtocols}
import ru.itclover.tsp.phases.NumericPhases.SymbolNumberExtractor

import scala.concurrent.ExecutionContextExecutor

object ValidationRoutes {

  def fromExecutionContext(
    monitoringUri: Uri
  )(implicit as: ActorSystem, am: ActorMaterializer): Reader[ExecutionContextExecutor, Route] =
    Reader { execContext =>
      new ValidationRoutes {
        //implicit override val executionContext = execContext
        //implicit override val actors = as
        //implicit override val materializer = am
        //override val uri = monitoringUri
      }.route
    }
}

trait ValidationRoutes extends RoutesProtocols with PatternsValidatorProtocols {

  val route: Route = path("patterns" / "validate"./) {
    entity(as[PatternsValidatorConf]) { request =>
      val patterns: Seq[String] = request.patterns
      val res = PatternsValidator.validate[Nothing](patterns)(
        new TimeExtractor[Nothing] { override def apply(v1: Nothing): Time = Time(0) },
        new SymbolNumberExtractor[Nothing] { override def extract(event: Nothing, symbol: Symbol): Double = 0.0 }
      )
      val result = res.map { x =>
        x._2 match {
          case Right(success) =>
            Map[String, Either[Boolean, String]](
              "pattern" -> Right(x._1),
              "success" -> Left(true),
              "result"  -> Right(success.toString)
            )
          case Left(error) =>
            Map[String, Either[Boolean, String]](
              "pattern" -> Right(x._1),
              "success" -> Left(false),
              "reason"  -> Right(error.toString)
            )
        }
      }
      complete(result)
    }
  }
}
