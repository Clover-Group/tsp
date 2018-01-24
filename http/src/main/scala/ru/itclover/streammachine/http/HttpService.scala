package ru.itclover.streammachine.http

import java.sql.Timestamp
import java.time.DateTimeException

import akka.actor.ActorSystem
import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.server.{Directives, ExceptionHandler, RequestContext, Route}
import akka.stream.ActorMaterializer
import ru.itclover.streammachine.http.domain.output.{FailureResponse, SuccessfulResponse}
import ru.itclover.streammachine.http.protocols.JsonProtocols
import ru.itclover.streammachine.http.routes.FindPatternRangesRoute
import ru.itclover.streammachine.io.input
import ru.itclover.streammachine.io.output
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import ru.itclover.streammachine.transformers.FlatMappersCombinator
import ru.itclover.streammachine.{ResultMapper, SegmentResultsMapper}
import com.typesafe.scalalogging.Logger
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import ru.itclover.streammachine.core.Aggregators.Segment
import ru.itclover.streammachine.core.NumericPhaseParser.SymbolNumberExtractor
import ru.itclover.streammachine.core.PhaseResult.{Failure, Success, TerminalResult}
import ru.itclover.streammachine.core.Time.TimeExtractor
import ru.itclover.streammachine.http.domain.input.FindPatternsRequest
import ru.itclover.streammachine.io.input.source.JDBCSourceInfo
import ru.itclover.streammachine.io.output.{ClickhouseOutput, JDBCSegmentsSink}
import ru.itclover.streammachine.transformers.FlinkStateCodeMachineMapper
import ru.itclover.streammachine.DataStreamUtils.DataStreamOps


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

  // SrcInfo -> Stream -> result

  override val route: Route =
    handleExceptions(defaultErrorsHandler) {
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

            val resultStream = stream.flatMapAll[Row](flatMappers)

            val chOutputFormat = ClickhouseOutput.getOutputFormat(outputConf)

            resultStream.addSink(new OutputFormatSinkFunction(chOutputFormat))

            onSuccess(Future { streamEnvironment.execute() }) {
              jobResult => complete(SuccessfulResponse(jobResult.hashCode))
            }
          }
        }
      }
    }

  private def getPackInRowMapper(schema: JDBCSegmentsSink, fieldsIndexesMap: Map[Symbol, Int], patternId: String)
                                (implicit timeExtractor: TimeExtractor[Row]) = new ResultMapper[Row, Segment, Row] {
      val segmentsMapper = SegmentResultsMapper[Row, Segment]

      override def apply(eventRow: Row, results: Seq[TerminalResult[Segment]]) = {
        segmentsMapper(eventRow, results) map {
          case (Success(segment)) => {
            val resultRow = new Row(schema.fieldsCount)
            // Throw exception here instead of Either or Try due to high computational intensity, O(n).
            val (fromTime, fromMillis) = segment.from.toString.split('.') match {
              case Array(time, millis, _*) => time -> millis.toInt
              case _ => throw new DateTimeException(s"Invalid date format: ${segment.from.toString}, valid format is: yyyy-MM-dd HH:mm:ss.SSS")
            }
            val (toTime, toMillis) = segment.to.toString.split('.') match {
              case Array(time, millis, _*) => time -> millis.toInt
              case _ => throw new DateTimeException(s"Invalid date format: ${segment.to.toString}, valid format is: yyyy-MM-dd HH:mm:ss.SSS")
            }

            resultRow.setField(schema.fromTimeInd, fromTime)
            resultRow.setField(schema.fromTimeMillisInd, fromMillis)
            resultRow.setField(schema.toTimeInd, toTime)
            resultRow.setField(schema.toTimeMillisInd, toMillis)
            resultRow.setField(schema.patternIdInd, patternId)
            schema.forwardedFields.foreach { case (field) =>
              val fieldValue = eventRow.getField(fieldsIndexesMap(field))
              resultRow.setField(schema.fieldsIndexesMap(field), fieldValue)
            }
            Success(resultRow)
          }
          case f@Failure(msg) => f
        }
      }
    }

}
