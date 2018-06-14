package ru.itclover.streammachine.http

import akka.actor.ActorSystem
import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server._
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import cats.data.Reader
import com.typesafe.config.ConfigFactory
import ru.itclover.streammachine.http.domain.output.{FailureResponse, SuccessfulResponse}
import ru.itclover.streammachine.http.routes.{JdbcStreamRoutes, JdbcToKafkaStreamRoute}
import scala.concurrent.ExecutionContextExecutor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import com.typesafe.scalalogging.Logger
import org.apache.flink.runtime.client.JobExecutionException
import ru.itclover.streammachine.http.protocols.JsonProtocols
import ru.itclover.streammachine.utils.Exceptions
import ru.yandex.clickhouse.except.ClickHouseException


trait HttpService extends JsonProtocols {
  val isDebug = true
  val isHideExceptions = ConfigFactory.load().getBoolean("general.is-hide-exceptions")

  implicit val system: ActorSystem
  implicit val materializer: ActorMaterializer
  implicit val streamEnvironment: StreamExecutionEnvironment
  implicit val executionContext: ExecutionContextExecutor

  private val log = Logger[HttpService]

  def composeRoutes: Reader[ExecutionContextExecutor, Route] = for {
    jdbcBatch <- JdbcStreamRoutes.fromExecutionContext
    kafkaStream <- JdbcToKafkaStreamRoute.fromExecutionContext
  } yield jdbcBatch ~ kafkaStream

  def route: Route = handleErrors {
    composeRoutes.run(executionContext).andThen { futureRoute =>
      futureRoute.onComplete { _ => System.gc() } // perform full GC after each route
      futureRoute
    }
  }


  def handleErrors: Directive[Unit] = handleRejections(rejectionsHandler) & handleExceptions(exceptionsHandler)

  def rejectionsHandler: RejectionHandler = RejectionHandler.newBuilder()
    .handleNotFound { extractUnmatchedPath { p =>
      complete(NotFound, FailureResponse(404, s"Path not found: `$p`", Seq.empty))
    }}
    .handleAll[MalformedFormFieldRejection] { x =>
      complete(BadRequest, FailureResponse(4001, s"Malformed field.", x.map(_.toString)))
    }
    .handleAll[MalformedQueryParamRejection] { x =>
      complete(BadRequest, FailureResponse(4002, s"Malformed query.", x.map(_.toString)))
    }
    .handleAll[MalformedRequestContentRejection] { x =>
      complete(BadRequest, FailureResponse(4003, s"Malformed request content.", x.map(_.toString)))
    }
    .handleAll[Rejection] { _ =>
      complete(InternalServerError, FailureResponse(5003, s"Unknown rejection.", Seq.empty))
    }.result()

  def exceptionsHandler = ExceptionHandler {
    case ex: ClickHouseException => // TODO Extract from jobs (ADT?)
      val stackTrace = Exceptions.getStackTrace(ex)
      val msg = if (ex.getCause != null) ex.getCause.getLocalizedMessage else ex.getMessage
      val error = s"Uncaught error during connection to Clickhouse, cause - `${msg}`, \n\nstacktrace: `$stackTrace`"
      log.error(error)
      complete(InternalServerError, FailureResponse(5001, s"Job execution failure",
        if (!isHideExceptions) Seq(error) else Seq.empty))
    case ex: JobExecutionException =>
      val stackTrace = Exceptions.getStackTrace(ex)
      val msg = if (ex.getCause != null) ex.getCause.getLocalizedMessage else ex.getMessage
      val error = s"Uncaught error during job execution, cause - `${msg}`, \n\nstacktrace: `$stackTrace`"
      log.error(error)
      complete(InternalServerError, FailureResponse(5002, s"Job execution failure",
        if (!isHideExceptions) Seq(error) else Seq.empty))
    case ex @ (_: RuntimeException | _: java.io.IOException)=>
      val stackTrace = Exceptions.getStackTrace(ex)
      val msg = if (ex.getCause != null) ex.getCause.getLocalizedMessage else ex.getMessage
      val error = s"Uncaught error during request handling, cause - `${msg}`, \n\nstacktrace: `$stackTrace`"
      log.error(error)
      complete(InternalServerError, FailureResponse(5005, s"Request handling failure",
        if (!isHideExceptions) Seq(error) else Seq.empty))
  }

}
