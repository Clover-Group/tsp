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
import ru.itclover.tsp.http.domain.output._
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
import ru.itclover.tsp.http.domain.output.SuccessfulResponse.ExecInfo
import ru.itclover.tsp.http.services.flink.MonitoringService
import ru.itclover.tsp.io.DecoderInstances
import ru.itclover.tsp.utils.UtilityTypes.ParseException
import ru.itclover.tsp.io.EventCreatorInstances.rowEventCreator
import ru.itclover.tsp.utils.ErrorsADT.{ConfigErr, GenericRuntimeErr, RuntimeErr}
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
        val resultOrErr = for {
          source <- JdbcSource.create(inputConf)
          // sink <- JDBCSink.create(outConf) // todo parallel validation of source and sink
          searcher = PatternsSearchJob(source, DecoderInstances)
          _ <- searcher.patternsSearchStream(
            patterns,
            outConf,
            PatternsToRowMapper(inputConf.sourceId, outConf.rowSchema)
          )
          execResult <- runStream(uuid, isAsync)
        } yield execResult

        resultOrErr match {
          case Left(err: ConfigErr) => complete(BadRequest, FailureResponse(err))
          case Left(err: RuntimeErr) => complete(InternalServerError, FailureResponse(err))
          case Right(Some(execResult)) => {
            // todo query read and written rows (onComplete(monitoring.queryJobInfo(request.uuid)))
            val execTime = execResult.getNetRuntime(TimeUnit.SECONDS)
            complete(SuccessfulResponse(ExecInfo(execTime, Map.empty)))
          }
          case Right(None) => complete(SuccessfulResponse(uuid, Seq(s"Job `$uuid` has started.")))

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

  def runStream(uuid: String, isAsync: Boolean): Either[RuntimeErr, Option[JobExecutionResult]] =
    if (isAsync) {
      Future { streamEnv.execute(uuid) } // TODO: possible deadlock for huge jobs number! Custom thread pool or something
      Right(None)
    } else {
      Either.catchNonFatal(Some(streamEnv.execute(uuid))).leftMap(GenericRuntimeErr(_))
    }

}
