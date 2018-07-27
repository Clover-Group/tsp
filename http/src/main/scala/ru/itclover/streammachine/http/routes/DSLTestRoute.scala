package ru.itclover.streammachine.http.routes

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import cats.data.Reader
import com.typesafe.scalalogging.Logger
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.parboiled2.{ErrorFormatter, ParseError}
import ru.itclover.streammachine.core.{PhaseParser, Time}
import ru.itclover.streammachine.core.Time.TimeExtractor
import ru.itclover.streammachine.http.domain.input.DSLPatternRequest
import ru.itclover.streammachine.http.domain.output.{FailureResponse, SuccessfulResponse}
import ru.itclover.streammachine.http.protocols.JsonProtocols
import ru.itclover.streammachine.newsyntax.PhaseBuilder
import ru.itclover.streammachine.phases.NumericPhases.SymbolNumberExtractor

import scala.concurrent.ExecutionContextExecutor
import scala.util.{Failure, Success}

object DSLTestRoute {
  def fromExecutionContext(implicit strEnv: StreamExecutionEnvironment): Reader[ExecutionContextExecutor, Route] =
    Reader { execContext =>
      new DSLTestRoute {
        override implicit val executionContext: ExecutionContextExecutor = execContext
        override implicit val streamEnv: StreamExecutionEnvironment = strEnv
      }.route
    }
}

trait DSLTestRoute extends JsonProtocols {
  implicit val system: ActorSystem = ActorSystem("my-system")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher
  implicit val streamEnv = StreamExecutionEnvironment.createLocalEnvironment()

  implicit val dummyTimeExtractor: TimeExtractor[Unit] = new TimeExtractor[Unit] {
    override def apply(v1: Unit): Time = Time(0)
  }

  implicit val dummySymbolNumberExtractor: SymbolNumberExtractor[Unit] = new SymbolNumberExtractor[Unit] {
    override def extract(event: Unit, symbol: Symbol): Double = Double.NaN
  }

  private val log = Logger[DSLTestRoute]

  val route: Route = path("streamJobDSL" / "validate-dsl") {
    entity(as[DSLPatternRequest]) { dslPatternRequest =>
      val (result, parser) = PhaseBuilder.build[Unit](dslPatternRequest.pattern)
      result match {
        case Success(x: PhaseParser[Unit, _, _]) =>
          val statusCode = StatusCodes.custom(901, "Test", "Test", isSuccess = true, allowsEntity = true)
          complete(statusCode, SuccessfulResponse("Parsing successful", Seq(s"parsed: ${x.formatWithInitialState(None)}")))
        case Failure(x: ParseError) =>
          complete(400, FailureResponse(400, "Parsing error", Seq(parser.formatError(x))))
        case Success(z) =>
          complete(500, FailureResponse(new Exception(z.toString)))
        case Failure(z) =>
          complete(500, FailureResponse(new Exception(z.toString)))
      }
    }
  }
}
