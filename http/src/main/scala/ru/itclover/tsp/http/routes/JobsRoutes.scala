package ru.itclover.tsp.http.routes

import java.util.concurrent.TimeUnit
import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.StatusCodes.{BadRequest, InternalServerError}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import akka.stream.ActorMaterializer
import com.typesafe.scalalogging.Logger
import org.apache.flink.streaming.api.scala._
import ru.itclover.tsp.http.domain.input.FindPatternsRequest
import ru.itclover.tsp.http.domain.output.{ExecInfo, FailureResponse, FinishedJobResponse, SuccessfulResponse}
import ru.itclover.tsp.http.protocols.RoutesProtocols
import ru.itclover.tsp.io.input.JDBCInputConf
import ru.itclover.tsp.io.output.{JDBCOutput, JDBCOutputConf, OutputConf, RowSchema}
import ru.itclover.tsp.transformers._
import ru.itclover.tsp.DataStreamUtils.DataStreamOps
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import cats.data.Reader
import cats.implicits._
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.types.Row
import ru.itclover.tsp.{PatternsSearchJob, PatternsToRowMapper, ResultMapper}
import ru.itclover.tsp.core.Pattern
import ru.itclover.tsp.dsl.schema.RawPattern
import ru.itclover.tsp.http.services.flink.MonitoringService
import cats.syntax.either._
import ru.itclover.tsp.io.DecoderInstances
import ru.itclover.tsp.utils.UtilityTypes.ParseException
import ru.itclover.tsp.io.EventCreatorInstances.rowEventCreator
import scala.util.Success


object JobsRoutes {

  def fromExecutionContext(monitoringUrl: Uri)(
    implicit strEnv: StreamExecutionEnvironment,
    as: ActorSystem,
    am: ActorMaterializer
  ): Reader[ExecutionContextExecutor, Route] =
    Reader { execContext =>
      new JobsRoutes {
        implicit val executionContext: ExecutionContextExecutor = execContext
        implicit val streamEnv: StreamExecutionEnvironment = strEnv
        implicit val actorSystem = as
        implicit val materializer = am
        override val monitoringUri = monitoringUrl
      }.route
    }
}

trait JobsRoutes extends RoutesProtocols {
  implicit val executionContext: ExecutionContextExecutor
  implicit def streamEnv: StreamExecutionEnvironment
  implicit val actorSystem: ActorSystem
  implicit val materializer: ActorMaterializer

  val monitoringUri: Uri
  lazy val monitoring = MonitoringService(monitoringUri)

  private val log = Logger[JobsRoutes]

  val route: Route = parameter('run_async.as[Boolean] ? true) { isAsync =>
    path("streamJob" / "from-jdbc" / "to-jdbc"./) {
      entity(as[FindPatternsRequest[JDBCInputConf, JDBCOutputConf]]) { request =>
        import request._
        val streamsOrErr = for {
          source <- JdbcSource.create(inputConf)
          searcher = PatternsSearchJob(source, DecoderInstances)
          sinks <- searcher.findAndSaveMappedIncidents(
            patterns,
            outConf,
            PatternsToRowMapper(inputConf.sourceId, outConf.rowSchema)
          )
          execResult <- if (isAsync) Right(None) else Either.catchNonFatal(Some(streamEnv.execute(uuid)))
        } yield execResult

        streamsOrErr match {
          case Right(Some(execResult)) => {
            // .. todo prepared patterns printing
            val execTime = execResult.getNetRuntime(TimeUnit.SECONDS)
            // todo onComplete(monitoring.queryJobInfo(request.uuid)) {
            complete(
              FinishedJobResponse(ExecInfo(execTime, Map.empty))
            )
          }
          case Right(None) => {
            Future { streamEnv.execute(uuid) } // TODO: possible deadlock! Custom thread pool needed
            complete(SuccessfulResponse(uuid, Seq(s"Job `$uuid` has started.")))
          }
        }
      }
    } /*~
    path("streamJob" / "from-influxdb" / "to-jdbc"./) {
      entity(as[FindPatternsRequest[InfluxDBInputConf, JDBCOutputConf]]) { request =>
        implicit val src = InfluxDBSource(request.source)
        implicit val (timeExtr, timeNTExtr, numExtr, anyExtr, anyNTExtr, kvExtr) = getExtractorsOrThrow[Row](request.source)
        val job = new PatternsSearchJob[Row, Any, Row](
          request.source,
          request.sink,
          PatternsToRowMapper(request.source.sourceId, request.sink.rowSchema)
        )
        runJob(job, src.emptyEvent, request.patterns, request.uuid, isAsync)
      }
    }*/
  }
  // TODO: Kafka outputConf

}
