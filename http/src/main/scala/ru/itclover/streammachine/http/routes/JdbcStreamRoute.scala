package ru.itclover.streammachine.http.routes

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import com.typesafe.scalalogging.Logger
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction
import org.apache.flink.streaming.api.scala._
import akka.http.scaladsl.model.StatusCodes._
import ru.itclover.streammachine.http.domain.input.FindPatternsRequest
import ru.itclover.streammachine.http.domain.output.{FailureResponse, SuccessfulResponse}
import ru.itclover.streammachine.http.protocols.JsonProtocols
import ru.itclover.streammachine.io.input.{InputConf, JDBCInputConf, RawPattern}
import ru.itclover.streammachine.io.output.{ClickhouseOutput, JDBCOutputConf, RowSchema}
import ru.itclover.streammachine.transformers.{PatternsSearchStages, StreamSources}
import ru.itclover.streammachine.DataStreamUtils.DataStreamOps
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import cats.data.Reader
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import ru.itclover.streammachine.utils.Time.timeIt
import ru.itclover.streammachine.http.utils.ImplicitUtils.RightBiasedEither


object JdbcStreamRoute {
  def fromExecutionContext(implicit strEnv: StreamExecutionEnvironment): Reader[ExecutionContextExecutor, Route] =
    Reader { execContext =>
      new JdbcStreamRoute {
        implicit val executionContext: ExecutionContextExecutor = execContext
        implicit val streamEnv: StreamExecutionEnvironment = strEnv
      }.route
    }
}


trait JdbcStreamRoute extends JsonProtocols {
  implicit val executionContext: ExecutionContextExecutor
  implicit def streamEnv: StreamExecutionEnvironment

  private val log = Logger[JdbcStreamRoute]

  val route: Route = path("streamJob" / "from-jdbc" / "to-jdbc" /) {
    entity(as[FindPatternsRequest[JDBCInputConf, JDBCOutputConf]]) { patternsRequest =>
      val (inputConf, outputConf, patterns) = (patternsRequest.source, patternsRequest.sink, patternsRequest.patterns)
      log.info(s"Starting patterns finding with input JDBC conf: `$inputConf`,\nOutput JDBC conf: `$outputConf`\n" +
        s"patterns codes: `$patterns`")

      val jobIdOrError = for {
        stream <- StreamSources.fromJdbc(inputConf)
        patterns <- PatternsSearchStages.findInRows(stream, inputConf, patterns,
          outputConf.rowSchema)(stream.dataType, streamEnv)
      } yield {
        val chOutputFormat = ClickhouseOutput.getOutputFormat(outputConf)
        patterns.addSink(new OutputFormatSinkFunction(chOutputFormat)).name("JDBC writing stage")
        timeIt { streamEnv.execute() }
      }

      jobIdOrError match {
        case Right(jobId) => complete(SuccessfulResponse(jobId.hashCode))
        case Left(err) => failWith(err) // TODO Mb complete(InternalServerError, FailureResponse(5004, err))
      }
    }
  }
}