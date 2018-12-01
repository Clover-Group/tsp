package ru.itclover.tsp.http.routes

import java.util.concurrent.TimeUnit
import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.StatusCodes.{BadRequest, InternalServerError}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{ExceptionHandler, Route, StandardRoute}
import akka.stream.ActorMaterializer
import com.typesafe.scalalogging.Logger
import org.apache.flink.streaming.api.scala._
import ru.itclover.tsp.http.domain.input.FindPatternsRequest
import ru.itclover.tsp.http.domain.output._
import ru.itclover.tsp.http.protocols.RoutesProtocols
import ru.itclover.tsp.io.input.{InfluxDBInputConf, JDBCInputConf}
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
import ru.itclover.tsp.io.AnyDecodersInstances
import ru.itclover.tsp.utils.UtilityTypes.ParseException
import ru.itclover.tsp.io.EventCreatorInstances.rowEventCreator
import ru.itclover.tsp.utils.ErrorsADT.{ConfigErr, Err, GenericRuntimeErr, RuntimeErr}
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
          source   <- JdbcSource.create(inputConf)
          searcher =  PatternsSearchJob(source, AnyDecodersInstances)
          _        <- searcher.patternsSearchStream(
            patterns,
            outConf,
            PatternsToRowMapper(inputConf.sourceId, outConf.rowSchema)
          )
          result   <- runStream(uuid, isAsync)
        } yield result

        matchResultToResponse(resultOrErr, uuid)
      }
    } ~
    path("streamJob" / "from-influxdb" / "to-jdbc"./) {
      entity(as[FindPatternsRequest[InfluxDBInputConf, JDBCOutputConf]]) { request =>
        import request._

        val resultOrErr = for {
          source   <- InfluxDBSource.create(inputConf)
          searcher =  PatternsSearchJob(source, AnyDecodersInstances)
          _        <- searcher.patternsSearchStream(
            patterns,
            outConf,
            PatternsToRowMapper(inputConf.sourceId, outConf.rowSchema)
          )
          result   <- runStream(uuid, isAsync)
        } yield result

        matchResultToResponse(resultOrErr, uuid)
      }
    }
  }

  def runStream(uuid: String, isAsync: Boolean): Either[RuntimeErr, Option[JobExecutionResult]] =
    if (isAsync) { // Just detach job thread in case of async run
      Future { streamEnv.execute(uuid) } // TODO: possible deadlocks for big jobs amount! Custom thread pool or something
      Right(None)
    } else {       // Wait for the execution finish
      Either.catchNonFatal(Some(streamEnv.execute(uuid))).leftMap(GenericRuntimeErr(_))
    }

  def matchResultToResponse(result: Either[Err, Option[JobExecutionResult]], uuid: String): StandardRoute =
    result match {
      case Left(err: ConfigErr)  => complete(BadRequest, FailureResponse(err))
      case Left(err: RuntimeErr) => complete(InternalServerError, FailureResponse(err))
      // Async job - response with message about successful start
      case Right(None) => complete(SuccessfulResponse(uuid, Seq(s"Job `$uuid` has started.")))
      // Sync job - response with message about successful ending
      case Right(Some(execResult)) => {
        // todo query read and written rows (onComplete(monitoring.queryJobInfo(request.uuid)))
        val execTime = execResult.getNetRuntime(TimeUnit.SECONDS)
        complete(SuccessfulResponse(ExecInfo(execTime, Map.empty)))
      }
    }

}
