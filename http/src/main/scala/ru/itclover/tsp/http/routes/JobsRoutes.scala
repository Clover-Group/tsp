package ru.itclover.tsp.http.routes

import java.util.concurrent.TimeUnit
import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes.{BadRequest, InternalServerError}
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.unmarshalling.FromRequestUnmarshaller
import akka.stream.ActorMaterializer
import cats.data.Reader
import cats.implicits._
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.types.StructType
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
import ru.itclover.tsp.spark
import org.apache.spark.sql.{Row => SparkRow}
import ru.itclover.tsp.spark.SparkStreamSource

import scala.concurrent.{ExecutionContextExecutor, Future}
import ru.itclover.tsp.spark.utils.DataWriterWrapper
import ru.itclover.tsp.streaming.utils.ErrorsADT.{ConfigErr => SparkConfErr, Err => SparkErr, GenericRuntimeErr => SparkGenRTErr, RuntimeErr => SparkRTErr}
import ru.itclover.tsp.spark.io.InputConf._
import ru.itclover.tsp.spark.io.OutputConf._
import ru.itclover.tsp.spark.utils.IndexerInstances.rowWithIdxIndexer
import ru.itclover.tsp.spark.utils.EmptyCheckerInstances.rowWithIdxEmptyChecker
import ru.itclover.tsp.streaming.StreamSource
import ru.itclover.tsp.streaming.io.{InputConf, OutputConf}
import ru.itclover.tsp.streaming.transformers.EmptyChecker
import ru.itclover.tsp.streaming.utils.{ErrorsADT, Indexer}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

// We use here Any and asInstanceOf. Probably cannot be done in other ways
@SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.AsInstanceOf"))
trait JobsRoutes extends RoutesProtocols {
  implicit val executionContext: ExecutionContextExecutor
  val blockingExecutionContext: ExecutionContextExecutor
  implicit val actorSystem: ActorSystem
  implicit val materializer: ActorMaterializer
  implicit val decoders = AnyDecodersInstances
  implicit val emptyChecker = rowWithIdxEmptyChecker

  val monitoringUri: Uri
  lazy val monitoring = FlinkMonitoringService(monitoringUri)

  @transient
  private val log = Logger[JobsRoutes]

  val route: Route = parameter('run_async.as[Boolean] ? true) { isAsync =>
    path("streamJob" / """from-(\w+)""".r / """to-(\w+)""".r./) {
      case (from, to) =>
        redirect("sparkJob/from-$from/to-$to/", StatusCodes.PermanentRedirect)
    } ~
    path("sparkJob" / """from-(\w+)""".r / """to-(\w+)""".r./) {
      case (from, to) =>
        val um = (from, to) match {
          case ("jdbc", "jdbc")   => as[FindPatternsRequest[JDBCInputConf, JDBCOutputConf]]
          case ("kafka", "jdbc")  => as[FindPatternsRequest[KafkaInputConf, JDBCOutputConf]]
          case ("jdbc", "kafka")  => as[FindPatternsRequest[JDBCInputConf, KafkaOutputConf]]
          case ("kafka", "kafka") => as[FindPatternsRequest[KafkaInputConf, KafkaOutputConf]]
          case _                  => null
        }
        if (um != null) {
          entity(
            um.asInstanceOf[FromRequestUnmarshaller[
              FindPatternsRequest[InputConf[spark.utils.RowWithIdx, Symbol, Any], OutputConf[SparkRow]]
            ]]
          ) { request =>
            import request._
            val fields = PatternFieldExtractor.extract(patterns)

            //          val resultOrErr: Either[Err, Option[Unit]] = for {
            //            source <- spark.JdbcSource.create(inputConf, fields)
            //            stream <- createSparkStream(patterns, fields, inputConf, outConf, source)
            //            result <- runSparkStream(stream, isAsync)
            //          } yield result

            val source: Either[SparkConfErr, SparkStreamSource[spark.utils.RowWithIdx, Symbol, Any, StructType]] = from match {
              case "jdbc" => spark.JdbcSource.create(inputConf.asInstanceOf[JDBCInputConf], fields)
              case "kafka" => spark.KafkaSource.create(inputConf.asInstanceOf[KafkaInputConf], fields)
            }
            val stream: Either[SparkErr, DataWriterWrapper[SparkRow]] =
              source.flatMap(createSparkStream(uuid, patterns, fields, inputConf, outConf, _))
            val result: Either[SparkErr, Option[Long]] = stream.flatMap(runSparkStream(_, isAsync))
            val resultOrErr = result

            matchSparkResultToResponse(resultOrErr, uuid)
          }
        } else {
          complete(404 -> s"The $from -> $to Spark job is not supported")
        }
    }
  }

  // TODO: Restore EKey type parameter
  type EKey = Symbol

  def createSparkStream[E: ClassTag: TypeTag, EItem](
    uuid: String,
    patterns: Seq[RawPattern],
    fields: Set[EKey],
    inputConf: InputConf[E, EKey, EItem],
    outConf: OutputConf[SparkRow],
    source: SparkStreamSource[E, EKey, EItem, StructType]
  )(implicit decoders: BasicDecoders[EItem], indexer: Indexer[E, Any], emptyChecker: EmptyChecker[E]): Either[ErrorsADT.Err, DataWriterWrapper[SparkRow]] = {
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
    implicit as: ActorSystem,
    am: ActorMaterializer
  ): Reader[ExecutionContextExecutor, Route] = {

    log.debug("fromExecutionContext started")

    Reader { execContext =>
      new JobsRoutes {
        val blockingExecutionContext = blocking
        implicit val executionContext: ExecutionContextExecutor = execContext
        implicit val actorSystem = as
        implicit val materializer = am
        override val monitoringUri = monitoringUrl
      }.route
    }

  }

  log.debug("fromExecutionContext finished")
}
