package ru.itclover.tsp.http.routes

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes.{BadRequest, InternalServerError}
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import cats.data.Reader
import cats.implicits._
import com.typesafe.scalalogging.Logger
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.types.Row
import ru.itclover.tsp._
import ru.itclover.tsp.core.{Incident, RawPattern}
import ru.itclover.tsp.core.io.{AnyDecodersInstances, BasicDecoders}
import ru.itclover.tsp.http.domain.input.FindPatternsRequest
import ru.itclover.tsp.core.io.{AnyDecodersInstances, BasicDecoders}
import ru.itclover.tsp.dsl.PatternFieldExtractor
import ru.itclover.tsp.http.domain.input.FindPatternsRequest
import ru.itclover.tsp.http.domain.output.SuccessfulResponse.ExecInfo
import ru.itclover.tsp.http.domain.output._
import ru.itclover.tsp.http.protocols.RoutesProtocols
import ru.itclover.tsp.http.services.flink.MonitoringService
import ru.itclover.tsp.io.input.{InfluxDBInputConf, InputConf, JDBCInputConf, RedisInputConf}
import ru.itclover.tsp.io.output.{JDBCOutputConf, KafkaOutputConf, OutputConf, RedisOutputConf}
import ru.itclover.tsp.mappers._
import ru.itclover.tsp.spark
import org.apache.spark.sql.{SparkSession, Row => SparkRow}

import scala.concurrent.{ExecutionContextExecutor, Future}
import ru.itclover.tsp.io.input.KafkaInputConf
import ru.itclover.tsp.utils.ErrorsADT.{ConfigErr, Err, GenericRuntimeErr, RuntimeErr}

trait JobsRoutes extends RoutesProtocols {
  implicit val executionContext: ExecutionContextExecutor
  val blockingExecutionContext: ExecutionContextExecutor
  implicit val streamEnv: StreamExecutionEnvironment
  implicit val actorSystem: ActorSystem
  implicit val materializer: ActorMaterializer
  implicit val decoders = AnyDecodersInstances

  val monitoringUri: Uri
  lazy val monitoring = MonitoringService(monitoringUri)

  private val log = Logger[JobsRoutes]

  val route: Route = parameter('run_async.as[Boolean] ? true) { isAsync =>
    path("streamJob" / "from-jdbc" / "to-jdbc"./) {
      entity(as[FindPatternsRequest[JDBCInputConf, JDBCOutputConf]]) { request =>
        import request._
        val fields = PatternFieldExtractor.extract(patterns)

        val resultOrErr = for {
          source <- JdbcSource.create(inputConf, fields)
          _      <- createStream(patterns, fields, inputConf, outConf, source)
          result <- runStream(uuid, isAsync)
        } yield result

        matchResultToResponse(resultOrErr, uuid)
      }
    } ~
    path("streamJob" / "from-influxdb" / "to-jdbc"./) {
      entity(as[FindPatternsRequest[InfluxDBInputConf, JDBCOutputConf]]) { request =>
        import request._
        val fields = PatternFieldExtractor.extract(patterns)

        val resultOrErr = for {
          source <- InfluxDBSource.create(inputConf, fields)
          _      <- createStream(patterns, fields, inputConf, outConf, source)
          result <- runStream(uuid, isAsync)
        } yield result

        matchResultToResponse(resultOrErr, uuid)
      }
    } ~
    path("streamJob" / "from-kafka" / "to-jdbc"./) {
      entity(as[FindPatternsRequest[KafkaInputConf, JDBCOutputConf]]) { request =>
        import request._
        val fields = PatternFieldExtractor.extract(patterns)

        val resultOrErr = for {
          source <- KafkaSource.create(inputConf, fields)
          _ = log.info("Kafka create done")
          _ <- createStream(patterns, fields, inputConf, outConf, source)
          _ = log.info("Kafka createStream done")
          result <- runStream(uuid, isAsync)
          _ = log.info("Kafka runStream done")
        } yield result

        matchResultToResponse(resultOrErr, uuid)
      }
    } ~
    path("streamJob" / "from-jdbc" / "to-kafka"./) {
      entity(as[FindPatternsRequest[JDBCInputConf, KafkaOutputConf]]) { request =>
        import request._
        val fields = PatternFieldExtractor.extract(patterns)

        val resultOrErr = for {
          source <- JdbcSource.create(inputConf, fields)
          _      <- createStream(patterns, fields, inputConf, outConf, source)
          result <- runStream(uuid, isAsync)
        } yield result

        matchResultToResponse(resultOrErr, uuid)
      }
    } ~
    path("streamJob" / "from-influxdb" / "to-kafka"./) {
      entity(as[FindPatternsRequest[InfluxDBInputConf, KafkaOutputConf]]) { request =>
        import request._
        val fields = PatternFieldExtractor.extract(patterns)

        val resultOrErr = for {
          source <- InfluxDBSource.create(inputConf, fields)
          _      <- createStream(patterns, fields, inputConf, outConf, source)
          result <- runStream(uuid, isAsync)
        } yield result

        matchResultToResponse(resultOrErr, uuid)
      }
    } ~
    path("streamJob" / "from-kafka" / "to-kafka"./) {
      entity(as[FindPatternsRequest[KafkaInputConf, KafkaOutputConf]]) { request =>
        import request._
        val fields = PatternFieldExtractor.extract(patterns)

        val resultOrErr = for {
          source <- KafkaSource.create(inputConf, fields)
          _ = log.info("Kafka create done")
          _ <- createStream(patterns, fields, inputConf, outConf, source)
          _ = log.info("Kafka createStream done")
          result <- runStream(uuid, isAsync)
          _ = log.info("Kafka runStream done")
        } yield result

        matchResultToResponse(resultOrErr, uuid)
      }
    } ~
    path("streamJob" / "from-redis" / "to-redis"./) {
      entity(as[FindPatternsRequest[RedisInputConf, RedisOutputConf]]) { request =>

        import request._
        val fields = PatternFieldExtractor.extract(patterns)

        val resultOrErr = for {
          source <- RedisSource.create(inputConf, fields)
          _ = log.info("Redis create done")
          _ <- createStream(patterns, fields, inputConf, outConf, source)
          _ = log.info("Redis createStream done")
          result <- runStream(uuid, isAsync)
          _ = log.info("Redis runStream done")
        } yield result

        matchResultToResponse(resultOrErr, uuid)

      }
    } ~
    // Spark job (currently JDBC-JDBC only)
      path("sparkJob" / "from-jdbc" / "to-jdbc"./) {
        entity(as[FindPatternsRequest[spark.io.JDBCInputConf, spark.io.JDBCOutputConf]]) { request =>
          import request._
          val fields = PatternFieldExtractor.extract(patterns)

          val resultOrErr = for {
            source <- spark.io.JdbcSource.create(inputConf, fields)
            _      <- createSparkStream(patterns, fields, inputConf, outConf, source)
            result <- runStream(uuid, isAsync)
          } yield result

          matchResultToResponse(resultOrErr, uuid)
        }
      }
  }

  // TODO: Restore EKey type parameter
  type EKey = Symbol

  def createStream[E: TypeInformation, EItem](
    patterns: Seq[RawPattern],
    fields: Set[EKey],
    inputConf: InputConf[E, EKey, EItem],
    outConf: OutputConf[Row],
    source: StreamSource[E, EKey, EItem]
  )(implicit decoders: BasicDecoders[EItem]) = {
    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    log.debug("createStream started")

    val searcher = PatternsSearchJob(source, fields, decoders)
    val strOrErr = searcher.patternsSearchStream(
      patterns,
      outConf,
      PatternsToRowMapper(inputConf.sourceId, outConf.rowSchema)
    )
    strOrErr.map {
      case (parsedPatterns, stream) =>
        // .. patternV2.format
        val strPatterns = parsedPatterns.map {
          case ((_, meta), _) =>
            /*p.format(source.emptyEvent) +*/
            s" ;; Meta=$meta"
        }
        log.debug(s"Parsed patterns:\n${strPatterns.mkString(";\n")}")
        stream
    }
  }

  def createSparkStream[E: TypeInformation, EItem](
                                               patterns: Seq[RawPattern],
                                               fields: Set[EKey],
                                               inputConf: spark.io.InputConf[E, EKey, EItem],
                                               outConf: spark.io.OutputConf[SparkRow],
                                               source: spark.StreamSource[E, EKey, EItem]
                                             )(implicit decoders: BasicDecoders[EItem]) = {
    //streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    log.debug("createStream started")

    import ru.itclover.tsp.spark.utils.EncoderInstances.sinkRowEncoder

    val searcher = spark.PatternsSearchJob(source, fields, decoders)
    val strOrErr = searcher.patternsSearchStream(
      patterns,
      outConf,
      (x: Incident) => spark.utils.PatternsToRowMapper(inputConf.sourceId, outConf.rowSchema).map(x)
    )
    strOrErr.map {
      case (parsedPatterns, stream) =>
        // .. patternV2.format
        val strPatterns = parsedPatterns.map {
          case ((_, meta), _) =>
            /*p.format(source.emptyEvent) +*/
            s" ;; Meta=$meta"
        }
        log.debug(s"Parsed patterns:\n${strPatterns.mkString(";\n")}")
        stream
    }
  }

  def runStream(uuid: String, isAsync: Boolean): Either[RuntimeErr, Option[JobExecutionResult]] = {
    log.debug("runStream started")

    val res = if (isAsync) { // Just detach job thread in case of async run
      Future { streamEnv.execute(uuid) }(blockingExecutionContext)
      Right(None)
    } else { // Wait for the execution finish
      Either.catchNonFatal(Some(streamEnv.execute(uuid))).leftMap(GenericRuntimeErr(_))
    }

    log.debug("runStream finished")
    res
  }

  def matchResultToResponse(result: Either[Err, Option[JobExecutionResult]], uuid: String): Route = {

    log.debug("matchResultToResponse started")

    val res = result match {
      case Left(err: ConfigErr) => complete((BadRequest, FailureResponse(err)))
      case Left(err: RuntimeErr) =>
        log.error("Error in processing", err.asInstanceOf[GenericRuntimeErr].ex)
        complete((InternalServerError, FailureResponse(err)))
      // Async job - response with message about successful start
      case Right(None) => complete(SuccessfulResponse(uuid, Seq(s"Job `$uuid` has started.")))
      // Sync job - response with message about successful ending
      case Right(Some(execResult)) => {
        // todo query read and written rows (onComplete(monitoring.queryJobInfo(request.uuid)))
        val execTime = execResult.getNetRuntime(TimeUnit.SECONDS)
        complete(SuccessfulResponse(ExecInfo(execTime, Map.empty)))
      }
    }
    log.debug("matchResultToResponse finished")

    res

  }

}

object JobsRoutes {

  private val log = Logger[JobsRoutes]

  def fromExecutionContext(monitoringUrl: Uri, blocking: ExecutionContextExecutor)(
    implicit strEnv: StreamExecutionEnvironment,
    as: ActorSystem,
    am: ActorMaterializer
  ): Reader[ExecutionContextExecutor, Route] = {

    log.debug("fromExecutionContext started")

    Reader { execContext =>
      new JobsRoutes {
        val blockingExecutionContext = blocking
        implicit val executionContext: ExecutionContextExecutor = execContext
        implicit val streamEnv: StreamExecutionEnvironment = strEnv
        implicit val actorSystem = as
        implicit val materializer = am
        override val monitoringUri = monitoringUrl
      }.route
    }

  }

  log.debug("fromExecutionContext finished")
}
