package ru.itclover.streammachine.http.routes

import java.sql.Timestamp
import java.time.DateTimeException
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import akka.stream.ActorMaterializer
import com.typesafe.scalalogging.Logger
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction
import org.apache.flink.types.Row
import ru.itclover.streammachine.{ResultMapper, SegmentResultsMapper}
import ru.itclover.streammachine.core.NumericPhaseParser.SymbolNumberExtractor
import ru.itclover.streammachine.core.Time.TimeExtractor
import ru.itclover.streammachine.http.domain.input.FindPatternsRequest
import ru.itclover.streammachine.http.domain.output.SuccessfulResponse
import ru.itclover.streammachine.http.protocols.JsonProtocols
import ru.itclover.streammachine.io.input.source.JDBCSourceInfo
import ru.itclover.streammachine.io.input.{InputConf, JDBCInputConf, JDBCNarrowInputConf}
import ru.itclover.streammachine.io.output.{ClickhouseOutput, JDBCOutputConf, JDBCSegmentsSink}
import ru.itclover.streammachine.transformers.{FlinkStateCodeMachineMapper, SparseRowsDataAccumulator}
import ru.itclover.streammachine.DataStreamUtils.DataStreamOps
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.concurrent.duration.Duration
import cats.data.Reader
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import ru.itclover.streammachine.core.Aggregators.Segment
import ru.itclover.streammachine.core.PhaseResult.{Failure, Success, TerminalResult}


object FindPatternRangesRoute {
  def fromExecutionContext(implicit strEnv: StreamExecutionEnvironment): Reader[ExecutionContextExecutor, Route] =
    Reader { execContext =>
      new FindPatternRangesRoute {
        implicit val executionContext: ExecutionContextExecutor = execContext
        implicit val streamEnv: StreamExecutionEnvironment = strEnv
      }.route
    }
}


trait FindPatternRangesRoute extends JsonProtocols {
  implicit val executionContext: ExecutionContextExecutor
  implicit def streamEnv: StreamExecutionEnvironment

  private val log = Logger[FindPatternRangesRoute]


  val route: Route = path("streaming" / "find-patterns" / "wide-dense-table" /) {
    entity(as[FindPatternsRequest[JDBCInputConf, JDBCOutputConf]]) { patternsRequest =>
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
        override def extract(event: Row, symbol: Symbol): Double = {
          event.getField(srcInfo.fieldsIndexesMap(symbol)).asInstanceOf[Double]
        }
      }

      val stream = streamEnv.createInput(srcInfo.inputFormat).keyBy(e =>
        e.getField(srcInfo.partitionIndex)
      )


      val flatMappers = patternsIdsAndCodes.map { case (patternId, patternCode) =>
        val packInMapper = getPackInRowMapper(outputConf.sinkSchema, srcInfo.fieldsIndexesMap, patternId)
        FlinkStateCodeMachineMapper[Row]((patternId, patternCode), srcInfo.fieldsIndexesMap,
          packInMapper.asInstanceOf[ResultMapper[Row, Any, Row]], inputConf.datetimeColname)
      }

      val resultStream = stream.flatMapAll[Row](flatMappers)

      val chOutputFormat = ClickhouseOutput.getOutputFormat(outputConf)

      resultStream.addSink(new OutputFormatSinkFunction(chOutputFormat))

      onSuccess(Future { streamEnv.execute() }) {
        jobResult => complete(SuccessfulResponse(jobResult.hashCode))
      }
    }
  } ~
    path("streaming" / "find-patterns" / "narrow-table" /) {
      entity(as[FindPatternsRequest[JDBCNarrowInputConf, JDBCOutputConf]]) { patternsRequest =>
        val (inputConf, outputConf, patternsIdsAndCodes) = (patternsRequest.source, patternsRequest.sink, patternsRequest.patternsIdsAndCodes)
        log.info(s"Starting patterns finding with input JDBC conf: `$inputConf`,\nOutput JDBC conf: `$outputConf`\n" +
          s"patterns codes: `$patternsIdsAndCodes`")

        val srcInfo = JDBCSourceInfo(inputConf.jdbcConf) match {
          case Right(typesInfo) => typesInfo
          case Left(err) => throw err
        }

        implicit val timeExtractor: TimeExtractor[Row] = new TimeExtractor[Row] {
          override def apply(row: Row) = {
            row.getField(srcInfo.fieldsIndexesMap(srcInfo.datetimeFieldName)).asInstanceOf[Timestamp]
          }
        }
        implicit val keyValExtractor: Row => (Symbol, Float) = (event: Row) =>
          (Symbol(event.getField(srcInfo.fieldsIndexesMap(inputConf.keyColname)).asInstanceOf[String]),
            event.getField(srcInfo.fieldsIndexesMap(inputConf.valColname)).asInstanceOf[Float])
        implicit val anyExtractor: (Row, Symbol) => Any = (event: Row, name: Symbol) =>
          event.getField(srcInfo.fieldsIndexesMap(name))

        val accumulator = SparseRowsDataAccumulator(srcInfo, inputConf)

        val accumulatedTimeExtractor: TimeExtractor[Row] = new TimeExtractor[Row] {
          override def apply(row: Row) = {
            row.getField(accumulator.fieldsIndexesMap(srcInfo.datetimeFieldName)).asInstanceOf[Timestamp]
          }
        }

        val stream = streamEnv.createInput(srcInfo.inputFormat)
          .keyBy(e => e.getField(srcInfo.partitionIndex))
          .flatMap(accumulator)
          .keyBy(e => e.getField(accumulator.fieldsIndexesMap(inputConf.jdbcConf.partitionColnames.head)))

        // TODO: Extract to Fn
        val flatMappers = patternsIdsAndCodes.map { case (patternId, patternCode) =>
          val packInMapper = getPackInRowMapper(outputConf.sinkSchema, accumulator.fieldsIndexesMap, patternId)(accumulatedTimeExtractor)
          FlinkStateCodeMachineMapper[Row]((patternId, patternCode), accumulator.fieldsIndexesMap,
            packInMapper.asInstanceOf[ResultMapper[Row, Any, Row]], inputConf.jdbcConf.datetimeColname)
        }

        val resultStream = stream.flatMapAll[Row](flatMappers)

        val chOutputFormat = ClickhouseOutput.getOutputFormat(outputConf)

        resultStream.addSink(new OutputFormatSinkFunction(chOutputFormat))

        onSuccess(Future { streamEnv.execute() }) {
          jobResult => complete(SuccessfulResponse(jobResult.hashCode))
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
