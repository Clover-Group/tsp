package ru.itclover.tsp.http.services

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpMethod, HttpMethods, HttpRequest, Uri}
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshal}
import akka.stream.Materializer

import scala.concurrent.{ExecutionContext, Future}

object AkkaHttpUtils {

  def queryToEither[L, R](fullUri: Uri, method: HttpMethod = HttpMethods.GET)(
    implicit unm: FromEntityUnmarshaller[Either[L, R]],
    as: ActorSystem,
    am: Materializer,
    ec: ExecutionContext
  ): Future[Either[L, R]] = {
    val raw = Http().singleRequest(HttpRequest(uri = fullUri, method = method))
    raw.flatMap { rs =>
      Unmarshal(rs.entity).to[Either[L, R]]
    }
  }
}
