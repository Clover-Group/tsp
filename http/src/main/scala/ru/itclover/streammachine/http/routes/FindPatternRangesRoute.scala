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
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import ru.itclover.streammachine.aggregators.AggregatorPhases.Segment
import ru.itclover.streammachine.core.PhaseParser
import ru.itclover.streammachine.EvalUtils


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
      val (inputConf, outputConf, patternsIdsAndCodes) = (patternsRequest.source, patternsRequest.sink, patternsRequest.patternsIdsAndCodes)
      log.info(s"Starting patterns finding with input JDBC conf: `$inputConf`,\nOutput JDBC conf: `$outputConf`\n" +
        s"patterns codes: `$patternsIdsAndCodes`")

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

      val flatMappers = patternsIdsAndCodes.map { case (patternId, patternCode) =>
        val packInMapper = SegmentResultsMapper[Row, Segment] andThen
          SegmentsToRowResultMapper[Row](outputConf.sinkSchema, patternId)
        val compilePhase = FindPatternRangesRoute.getPhaseCompiler(patternCode, srcInfo.datetimeFieldName,
          srcInfo.fieldsIndexesMap)(_)
        FlinkStateCodeMachineMapper[Row](compilePhase, packInMapper.asInstanceOf[ResultMapper[Row, Any, Row]],
          inputConf.eventsMaxGapMs, FindPatternRangesRoute.isTerminal(nullIndex)(_))
      }

      val resultStream = stream.keyBy(e => e.getField(srcInfo.partitionIndex)).flatMapAll[Row](flatMappers)

      val chOutputFormat = ClickhouseOutput.getOutputFormat(outputConf)

      resultStream.addSink(new OutputFormatSinkFunction(chOutputFormat))

      def time[R](block: => R): R = {
          val t0 = System.nanoTime()
          val result = block    // call-by-name
          val t1 = System.nanoTime()
          println("Elapsed time: " + (t1 - t0) + "ns")
          result
      }

      onSuccess(Future { time { streamEnv.execute() } }) {
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
        val timeIndex = accumulator.fieldsIndexesMap(srcInfo.datetimeFieldName)
        val partitionIndex = accumulator.fieldsIndexesMap(srcInfo.config.partitionColnames.head)

        val accumulatedTimeExtractor: TimeExtractor[Row] = new TimeExtractor[Row] {
          override def apply(row: Row) = {
            row.getField(timeIndex).asInstanceOf[Timestamp]
          }
        }

        val stream = streamEnv.createInput(srcInfo.inputFormat)
          .keyBy(e => e.getField(srcInfo.partitionIndex))
          .flatMap(accumulator)

        val nullIndex = accumulator.fieldsIndexesMap.find { case (_, ind) => ind != timeIndex && ind != partitionIndex } match {
          case Some((_, nullInd)) => nullInd
          case None =>
            throw new IllegalArgumentException(s"Query contains only date and partition columns: `${inputConf.jdbcConf.query}`")
        }

        val flatMappers = patternsIdsAndCodes.map { case (patternId, patternCode) =>
          def accumulatedAnyExtractor(event: Row, name: Symbol) = event.getField(accumulator.fieldsIndexesMap(name))
          val packInMapper = SegmentResultsMapper[Row, Segment]()(accumulatedTimeExtractor) andThen
            SegmentsToRowResultMapper[Row](outputConf.sinkSchema, patternId)(accumulatedTimeExtractor, accumulatedAnyExtractor)
          val compilePhase = FindPatternRangesRoute.getPhaseCompiler(patternCode, srcInfo.datetimeFieldName,
            accumulator.fieldsIndexesMap)(_)
          FlinkStateCodeMachineMapper[Row](compilePhase, packInMapper.asInstanceOf[ResultMapper[Row, Any, Row]],
            inputConf.jdbcConf.eventsMaxGapMs, FindPatternRangesRoute.isTerminal(nullIndex))(accumulatedTimeExtractor)
        }

        val resultStream = stream.keyBy(e => e.getField(partitionIndex))
                                 .flatMapAll[Row](flatMappers)

        val chOutputFormat = ClickhouseOutput.getOutputFormat(outputConf)

        resultStream.addSink(new OutputFormatSinkFunction(chOutputFormat))

        onSuccess(Future { streamEnv.execute() }) {
          jobResult => complete(SuccessfulResponse(jobResult.hashCode))
        }
      }
    }
}
