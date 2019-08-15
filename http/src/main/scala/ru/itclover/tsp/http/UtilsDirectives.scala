package ru.itclover.tsp.http

import akka.event.LoggingAdapter
import akka.event.Logging.LogLevel
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.{Directive, RejectionHandler, Route}
import akka.http.scaladsl.server.directives.{DebuggingDirectives, LogEntry, LoggingMagnet}
import akka.http.scaladsl.server.RouteResult.Complete
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.scaladsl.Sink
import ch.qos.logback.classic.encoder.PatternLayoutEncoder
import akka.http.scaladsl.server.Directives._
import com.typesafe.scalalogging.Logger
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

object UtilsDirectives {

  def logRequest(logFn: String => Unit)(implicit rejectionHandler: RejectionHandler): Directive[Unit] =
    extractRequestContext.flatMap { ctx =>
      mapRequest { req =>
        logFn(requestToString(req))
        req
      } & handleRejections(rejectionHandler) // handling rejections for proper status codes
    }

  def logResponse(logFn: String => Unit)(implicit rejHandler: RejectionHandler): Directive[Unit] = {
    extractRequestContext.flatMap { ctx =>
      val start = System.currentTimeMillis()
      mapResponse { resp =>
        val d = System.currentTimeMillis() - start
        logFn(responseToString(resp) + s" took ${d}ms, for request: ${ctx.request.method} ${ctx.request.uri}")
        resp
      } & handleRejections(rejHandler) // handling rejections for proper status codes
    }
  }

  def requestToString(r: HttpRequest): String = s"HttpRequest(\n\tmethod=${r._1},\n\tURI=`${r._2}`," +
  s"\n\theaders=`${r._3}`,\n\tentity=`${r._4}`,\n\tprotocol=`${r._5}`\n)"

  def responseToString(r: HttpResponse): String = s"HttpResponse(\n\tstatus=${r._1},\n\theaders=`${r._2}`," +
  s"\n\tentity=`${r._3}`,\n\tprotocol=${r._4}\n)"

  def entityAsString(entity: HttpEntity)(implicit m: Materializer, ex: ExecutionContext): Future[String] = {
    val charset = entity.getContentType().getCharsetOption.orElse(HttpCharsets.`UTF-8`)
    entity.dataBytes
      .map(_.decodeString(charset.nioCharset()))
      .runWith(Sink.head)
  }
}
