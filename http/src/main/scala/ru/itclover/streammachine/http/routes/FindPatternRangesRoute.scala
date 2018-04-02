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
import ru.itclover.streammachine.{ResultMapper, SegmentResultsMapper, SegmentsToRowResultMapper}
import ru.itclover.streammachine.phases.NumericPhases.SymbolNumberExtractor
import ru.itclover.streammachine.core.Time.TimeExtractor
import ru.itclover.streammachine.http.domain.input.FindPatternsRequest
import ru.itclover.streammachine.http.domain.output.{FailureResponse, SuccessfulResponse}
import ru.itclover.streammachine.http.protocols.JsonProtocols
import ru.itclover.streammachine.io.input.source.JDBCSourceInfo
import ru.itclover.streammachine.io.input.{InputConf, JDBCInputConf, JDBCNarrowInputConf}
import ru.itclover.streammachine.io.output.{ClickhouseOutput, JDBCOutputConf, PGSegmentsSink}
import ru.itclover.streammachine.transformers.{FlinkStateCodeMachineMapper, SparseRowsDataAccumulator}
import ru.itclover.streammachine.DataStreamUtils.DataStreamOps
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.concurrent.duration.Duration
import cats.data.Reader
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import ru.itclover.streammachine.aggregators.AggregatorPhases.Segment
import ru.itclover.streammachine.core.PhaseParser
import ru.itclover.streammachine.EvalUtils
import ru.itclover.streammachine.utils.Time.timeIt

import scala.util.{Failure, Success}


object FindPatternRangesRoute {
  def fromExecutionContext(implicit strEnv: StreamExecutionEnvironment): Reader[ExecutionContextExecutor, Route] =
    Reader { execContext =>
      new FindPatternRangesRoute {
        implicit val executionContext: ExecutionContextExecutor = execContext
        implicit val streamEnv: StreamExecutionEnvironment = strEnv
      }.route
    }

  // Todo private
  def getPhaseCompiler(code: String, timestampField: Symbol, fieldIndexesMap: Map[Symbol, Int])
                      (classLoader: ClassLoader): PhaseParser[Row, Any, Any] = {
    val evaluator = new EvalUtils.Eval(classLoader)
    evaluator.apply[(PhaseParser[Row, Any, Any])](
      EvalUtils.composePhaseCodeUsingRowExtractors(code, timestampField, fieldIndexesMap)
    )
  }

  def createTerminalRow(fieldNames: Seq[Symbol], partitionIndVal: (Int, Any), timeIndVal: (Int, Timestamp)): Row = {
    val r = new Row(fieldNames.length)
    fieldNames.indices.foreach(i => r.setField(i, null))
    r.setField(timeIndVal._1, timeIndVal._2)
    r.setField(partitionIndVal._1, partitionIndVal._2)
    r
  }

  def isTerminal(nullInd: Int)(row: Row) = row.getArity > nullInd && row.getField(nullInd) == null
}


trait FindPatternRangesRoute extends JsonProtocols {
  implicit val executionContext: ExecutionContextExecutor
  implicit def streamEnv: StreamExecutionEnvironment

  val terminalEventDate = "2030-01-01 00:00:00"
  private val log = Logger[FindPatternRangesRoute]

  val route: Route = path("streaming" / "find-patterns" / "wide-dense-table" /) {
    entity(as[FindPatternsRequest[JDBCInputConf, JDBCOutputConf]]) { patternsRequest =>
      val (inputConf, outputConf, patterns) = (patternsRequest.source, patternsRequest.sink, patternsRequest.patterns)
      log.info(s"Starting patterns finding with input JDBC conf: `$inputConf`,\nOutput JDBC conf: `$outputConf`\n" +
        s"patterns codes: `$patterns`")

      val srcInfo = JDBCSourceInfo(inputConf) match {
        case Right(typesInfo) => typesInfo
        case Left(err) => throw err
      }
      val timeInd = srcInfo.fieldsIndexesMap(srcInfo.datetimeFieldName)

      implicit val timeExtractor: TimeExtractor[Row] = new TimeExtractor[Row] {
        override def apply(row: Row) = {
          row.getField(timeInd).asInstanceOf[Timestamp]
        }
      }
      implicit val symbolNumberExtractorRow: SymbolNumberExtractor[Row] = new SymbolNumberExtractor[Row] {
        override def extract(event: Row, symbol: Symbol): Double = {
          event.getField(srcInfo.fieldsIndexesMap(symbol)).asInstanceOf[Double]
        }
      }
      implicit val anyExtractor: (Row, Symbol) => Any = (event: Row, name: Symbol) =>
        event.getField(srcInfo.fieldsIndexesMap(name))

      val nullIndex = srcInfo.fieldsIndexesMap.find {
        case (_, ind) => ind != timeInd && ind != srcInfo.partitionIndex
      } match {
        case Some((_, nullInd)) => nullInd
        case None =>
          throw new IllegalArgumentException(s"Query contains only date and partition columns: `${inputConf.query}`")
      }

      val stream = streamEnv.createInput(srcInfo.inputFormat)

      val flatMappers = patterns.map { pattern =>
        val packInMapper = SegmentResultsMapper[Row, Segment] andThen
          SegmentsToRowResultMapper[Row](inputConf.id, outputConf.sinkSchema, pattern)
        val compilePhase = FindPatternRangesRoute.getPhaseCompiler(pattern.sourceCode, srcInfo.datetimeFieldName,
          srcInfo.fieldsIndexesMap)(_)
        FlinkStateCodeMachineMapper[Row](compilePhase, packInMapper.asInstanceOf[ResultMapper[Row, Any, Row]],
          inputConf.eventsMaxGapMs, FindPatternRangesRoute.isTerminal(nullIndex)(_))
      }

      val resultStream = stream.keyBy(e => e.getField(srcInfo.partitionIndex))
                               .flatMapAll[Row](flatMappers)
                               .name("Rules searching stage")

      val chOutputFormat = ClickhouseOutput.getOutputFormat(outputConf)

      resultStream.addSink(new OutputFormatSinkFunction(chOutputFormat)).name("JDBC writing stage")

      val jobId = timeIt { streamEnv.execute() }

      complete(SuccessfulResponse(jobId.hashCode))
    }
  }
}
