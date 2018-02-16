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
<<<<<<< HEAD
import ru.itclover.streammachine.http.protocols.JsonProtocols
import scala.io.StdIn
=======
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import ru.itclover.streammachine.aggregators.AggregatorPhases.Segment
import ru.itclover.streammachine.core.PhaseResult.{Failure, Success, TerminalResult}
import ru.itclover.streammachine.core.Time.TimeExtractor
import ru.itclover.streammachine.http.domain.input.FindPatternsRequest
import ru.itclover.streammachine.io.input.source.JDBCSourceInfo
import ru.itclover.streammachine.io.output.{ClickhouseOutput, JDBCSegmentsSink}
import ru.itclover.streammachine.transformers.FlinkStateCodeMachineMapper
import ru.itclover.streammachine.DataStreamUtils.DataStreamOps
import ru.itclover.streammachine.phases.NumericPhases.SymbolNumberExtractor

import scala.collection.immutable


trait HttpService extends Directives with JsonProtocols with FindPatternRangesRoute {
  val isDebug = false
  val log = Logger[HttpService]

  val defaultErrorsHandler: ExceptionHandler = ExceptionHandler {
    case ex: Exception =>
      ex.printStackTrace()
      val reason = if (isDebug) ex.getMessage else ""
      complete(FailureResponse(-1, "Internal server error", reason :: Nil))
  }

  implicit def streamEnvironment: StreamExecutionEnvironment

  implicit def serviceRejections =
    RejectionHandler.newBuilder()
      .handle { case ValidationRejection(msg, cause) =>
        val reason = cause.getOrElse(new IllegalArgumentException("")).getMessage
        complete(FailureResponse(StatusCodes.InternalServerError.intValue, msg, reason :: Nil))
      }
      .handleNotFound {
        complete(FailureResponse(StatusCodes.NotFound.intValue, "Not found.", Seq.empty))
      }
      .handleAll[Rejection] { _ =>
        complete(FailureResponse(StatusCodes.MethodNotAllowed.intValue, "Not allowed.", Seq.empty))
      }
      .result()


  override val route: Route =
     handleRejections(serviceRejections) { handleExceptions(defaultErrorsHandler) {
      path("streaming" / "find-patterns" / "wide-dense-table" /) {
        requestEntityPresent {
          entity(as[FindPatternsRequest]) { patternsRequest =>
            val (inputConf, outputConf, patternsIdsAndCodes) = (patternsRequest.source, patternsRequest.sink, patternsRequest.patternsIdsAndCodes)
            log.info(s"Starting patterns finding with input JDBC conf: `$inputConf`,\nOutput JDBC conf: `$outputConf`\n" +
              s"patterns codes: `$patternsIdsAndCodes`")

            val srcInfo = JDBCSourceInfo(inputConf) match {
              case Right(typesInfo) => typesInfo
              case Left(err) => throw err
            }

            implicit val timeExtractor: TimeExtractor[Row] = new TimeExtractor[Row] {
              override def apply(row: Row) = {
                row.getField(srcInfo.fieldsIndexesMap(srcInfo.datetimeFieldName)).asInstanceOf[Timestamp]
              }
            }
            implicit val symbolNumberExtractorRow: SymbolNumberExtractor[Row] = new SymbolNumberExtractor[Row] {
              // TODO: Make it serializable
              override def extract(event: Row, symbol: Symbol): Double = {
                event.getField(srcInfo.fieldsIndexesMap(symbol)).asInstanceOf[Double]
              }
            }

            val stream = streamEnvironment.createInput(srcInfo.inputFormat).keyBy(e => e.getField(srcInfo.partitionIndex))


            val flatMappers = patternsIdsAndCodes.map { case (patternId, patternCode) =>
              val packInMapper = getPackInRowMapper(outputConf.sinkSchema, srcInfo.fieldsIndexesMap, patternId)
              FlinkStateCodeMachineMapper[Row]((patternId, patternCode), srcInfo.fieldsIndexesMap,
                packInMapper.asInstanceOf[ResultMapper[Row, Any, Row]], inputConf.datetimeColname)
            }
>>>>>>> origin/refactoring


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

  def route = handleErrors {
    composeRoutes.run(executionContext)
  }


  def handleErrors: Directive[Unit] = handleRejections(rejectionsHandler) & handleExceptions(exceptionsHandler)

  def rejectionsHandler: RejectionHandler = RejectionHandler.newBuilder()
    .handleNotFound {
      extractUnmatchedPath { p =>
        val msg = s"Path not found: $p"
        complete(FailureResponse(0, msg, Seq.empty))
      }
    }
    .handleAll[Rejection] { x =>
      complete(FailureResponse(StatusCodes.InternalServerError))
    }.result()


  def exceptionsHandler = ExceptionHandler {
    case ex: Exception =>
      ex.printStackTrace()
      complete(FailureResponse(ex))
  }
}
