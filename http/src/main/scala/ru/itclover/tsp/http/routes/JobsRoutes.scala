package ru.itclover.tsp.http.routes

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes.{BadRequest, InternalServerError}
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.unmarshalling.FromRequestUnmarshaller
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
import ru.itclover.tsp.http.services.streaming.FlinkMonitoringService
import ru.itclover.tsp.io.input.{InfluxDBInputConf, InputConf, JDBCInputConf}
import ru.itclover.tsp.io.output.{JDBCOutputConf, KafkaOutputConf, OutputConf}
import ru.itclover.tsp.mappers._
import ru.itclover.tsp.spark
import org.apache.spark.sql.{Row => SparkRow}

import scala.concurrent.{ExecutionContextExecutor, Future}
import ru.itclover.tsp.io.input.KafkaInputConf
import ru.itclover.tsp.spark.utils.{DataWriterWrapper, ErrorsADT}
import ru.itclover.tsp.utils.ErrorsADT.{ConfigErr, Err, GenericRuntimeErr, RuntimeErr}
import ru.itclover.tsp.spark.utils.ErrorsADT.{
  ConfigErr => SparkConfErr,
  Err => SparkErr,
  GenericRuntimeErr => SparkGenRTErr,
  RuntimeErr => SparkRTErr
}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

// We use here Any and asInstanceOf. Probably cannot be done in other ways
@SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.AsInstanceOf"))
trait JobsRoutes extends RoutesProtocols {
  implicit val executionContext: ExecutionContextExecutor
  val blockingExecutionContext: ExecutionContextExecutor
  implicit val streamEnv: StreamExecutionEnvironment
  implicit val actorSystem: ActorSystem
  implicit val materializer: ActorMaterializer
  implicit val decoders = AnyDecodersInstances

  val monitoringUri: Uri
  lazy val monitoring = FlinkMonitoringService(monitoringUri)

  @transient
  private val log = Logger[JobsRoutes]

  val route: Route = parameter('run_async.as[Boolean] ? true) { isAsync =>
    path("streamJob" / """from-(\w+)""".r / """to-(\w+)""".r./) {
      case (from, to) =>
        val um = (from, to) match {
          case ("jdbc", "jdbc")      => as[FindPatternsRequest[JDBCInputConf, JDBCOutputConf]]
          case ("influxdb", "jdbc")  => as[FindPatternsRequest[InfluxDBInputConf, JDBCOutputConf]]
          case ("kafka", "jdbc")     => as[FindPatternsRequest[KafkaInputConf, JDBCOutputConf]]
          case ("jdbc", "kafka")     => as[FindPatternsRequest[JDBCInputConf, KafkaOutputConf]]
          case ("influxdb", "kafka") => as[FindPatternsRequest[InfluxDBInputConf, KafkaOutputConf]]
          case ("kafka", "kafka")    => as[FindPatternsRequest[KafkaInputConf, KafkaOutputConf]]
          case _                     => null // Not implemented, will crash with a 500
        }
        entity(
          um.asInstanceOf[FromRequestUnmarshaller[
            FindPatternsRequest[InputConf[RowWithIdx, Symbol, Any], OutputConf[Row]]
          ]]
        ) { request: FindPatternsRequest[InputConf[RowWithIdx, Symbol, Any], OutputConf[Row]] =>
          import request._
          val fields = PatternFieldExtractor.extract(patterns)

          val srcOrError: Either[Err, StreamSource[RowWithIdx, Symbol, Any]] = from match {
            case "jdbc"     => JdbcSource.create(inputConf.asInstanceOf[JDBCInputConf], fields)
            case "influxdb" => InfluxDBSource.create(inputConf.asInstanceOf[InfluxDBInputConf], fields)
            case "kafka"    => KafkaSource.create(inputConf.asInstanceOf[KafkaInputConf], fields)
            //case _ => Left(ConfigErr)
          }

          val resultOrErr = for {
            source <- srcOrError
            _      <- createStream(patterns, /*fields,*/ inputConf, outConf, source)
            result <- runStream(uuid, isAsync)
          } yield result

          matchResultToResponse(resultOrErr, uuid)
        }
    } ~
    path("sparkJob" / """from-(\w+)""".r / """to-(\w+)""".r./) {
      case (from, to) =>
        val um = (from, to) match {
          case ("jdbc", "jdbc")  => as[FindPatternsRequest[spark.io.JDBCInputConf, spark.io.JDBCOutputConf]]
          case ("kafka", "jdbc") => as[FindPatternsRequest[spark.io.KafkaInputConf, spark.io.JDBCOutputConf]]
          case _                 => null // Not implemented, will crash with a 500
        }
        entity(
          um.asInstanceOf[FromRequestUnmarshaller[
            FindPatternsRequest[spark.io.InputConf[spark.utils.RowWithIdx, Symbol, Any], spark.io.OutputConf[SparkRow]]
          ]]
        ) { request =>
          import request._
          val fields = PatternFieldExtractor.extract(patterns)

          //          val resultOrErr: Either[Err, Option[Unit]] = for {
          //            source <- spark.JdbcSource.create(inputConf, fields)
          //            stream <- createSparkStream(patterns, fields, inputConf, outConf, source)
          //            result <- runSparkStream(stream, isAsync)
          //          } yield result

          val source: Either[SparkConfErr, spark.StreamSource[spark.utils.RowWithIdx, Symbol, Any]] = from match {
            case "jdbc"  => spark.JdbcSource.create(inputConf.asInstanceOf[spark.io.JDBCInputConf], fields)
            case "kafka" => spark.KafkaSource.create(inputConf.asInstanceOf[spark.io.KafkaInputConf], fields)
          }
          val stream: Either[SparkErr, DataWriterWrapper[SparkRow]] =
            source.flatMap(createSparkStream(uuid, patterns, fields, inputConf, outConf, _))
          val result: Either[SparkErr, Option[Long]] = stream.flatMap(runSparkStream(_, isAsync))
          val resultOrErr = result

          matchSparkResultToResponse(resultOrErr, uuid)
        }
    }
  }

  // TODO: Restore EKey type parameter
  type EKey = Symbol

  def createStream[E: TypeInformation, EItem](
    patterns: Seq[RawPattern],
    inputConf: InputConf[E, EKey, EItem],
    outConf: OutputConf[Row],
    source: StreamSource[E, EKey, EItem]
  )(implicit decoders: BasicDecoders[EItem]) = {
    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    log.debug("createStream started")

    val searcher = PatternsSearchJob(source, decoders)
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

  def createSparkStream[E: ClassTag: TypeTag, EItem](
    uuid: String,
    patterns: Seq[RawPattern],
    fields: Set[EKey],
    inputConf: spark.io.InputConf[E, EKey, EItem],
    outConf: spark.io.OutputConf[SparkRow],
    source: spark.StreamSource[E, EKey, EItem]
  )(implicit decoders: BasicDecoders[EItem]): Either[ErrorsADT.Err, DataWriterWrapper[SparkRow]] = {
    //streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    log.debug("createStream started")

    val searcher = spark.PatternsSearchJob(uuid, source, fields, decoders)
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

  def runSparkStream(stream: DataWriterWrapper[SparkRow], isAsync: Boolean): Either[SparkErr, Option[Long]] = {
    log.debug("runStream started")

    val res = if (isAsync) { // Just detach job thread in case of async run
      Future {
        val start = System.nanoTime
        stream.write()
        val end = System.nanoTime
        end - start
      }(blockingExecutionContext)
      Right(None)
    } else { // Wait for the execution finish
      Either
        .catchNonFatal {
          val start = System.nanoTime
          stream.write()
          val end = System.nanoTime
          Some(end - start)
        }
        .leftMap(SparkGenRTErr(_))
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
        complete(SuccessfulResponse(ExecInfo(execTime.toDouble, Map.empty)))
      }
    }
    log.debug("matchResultToResponse finished")

    res

  }

  def matchSparkResultToResponse(result: Either[SparkErr, Option[Long]], uuid: String): Route = {

    log.debug("matchResultToResponse started")

    val res = result match {
      case Left(err: SparkConfErr) => complete((BadRequest, FailureResponse(err)))
      case Left(err: SparkRTErr) =>
        log.error("Error in processing", err.asInstanceOf[SparkGenRTErr].ex)
        complete((InternalServerError, FailureResponse(err)))
      // Async job - response with message about successful start
      case Right(None) => complete(SuccessfulResponse(uuid, Seq(s"Job `$uuid` has started.")))
      // Sync job - response with message about successful ending
      case Right(Some(value)) =>
        val execTime = value * 1e-9 // execResult.getNetRuntime(TimeUnit.SECONDS)
        complete(SuccessfulResponse(ExecInfo(execTime, Map.empty)))
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
