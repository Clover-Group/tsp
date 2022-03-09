package ru.itclover.tsp.http.services.queuing

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.{HttpRequest, Uri}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import boopickle.Default._
import com.typesafe.scalalogging.Logger
import org.apache.flink.api.common.{JobExecutionResult, JobID}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.types.Row
import ru.itclover.tsp.core.RawPattern
import ru.itclover.tsp.{InfluxDBSource, JdbcSource, KafkaSource, PatternsSearchJob, RowWithIdx, StreamSource}
import ru.itclover.tsp.core.io.{AnyDecodersInstances, BasicDecoders}
import ru.itclover.tsp.dsl.PatternFieldExtractor
import ru.itclover.tsp.http.domain.input.{FindPatternsRequest, QueueableRequest}
import ru.itclover.tsp.http.routes.JobReporting
import ru.itclover.tsp.http.protocols.RoutesProtocols
import ru.itclover.tsp.http.services.streaming.MonitoringServiceModel.{JobDetails, Vertex, VertexMetrics}
import ru.itclover.tsp.http.services.streaming.{ConsoleStatusReporter, StatusReporter}
import ru.itclover.tsp.io.input.{InfluxDBInputConf, InputConf, JDBCInputConf, KafkaInputConf}
import ru.itclover.tsp.io.output.{JDBCOutputConf, KafkaOutputConf, OutputConf}
import ru.itclover.tsp.mappers.PatternsToRowMapper
import ru.itclover.tsp.utils.ErrorsADT.RuntimeErr
import spray.json._
import swaydb.Glass
import swaydb.persistent.{Set => PersistentSet}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.serializers.Serializer

import java.nio.file.Paths
import java.util.concurrent.{ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}
import scala.collection.mutable
import scala.concurrent.duration.{Duration, MILLISECONDS, SECONDS}
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

class QueueManagerService(uri: Uri, blockingExecutionContext: ExecutionContextExecutor)(
  implicit executionContext: ExecutionContextExecutor,
  streamEnv: StreamExecutionEnvironment,
  actorSystem: ActorSystem,
  materializer: ActorMaterializer,
  decoders: BasicDecoders[Any] = AnyDecodersInstances,
  reporting: Option[JobReporting],
  typeInfoRowWithIdx: TypeInformation[RowWithIdx],
  typeInfoRow: TypeInformation[Row]
) extends SprayJsonSupport
    with DefaultJsonProtocol {
  import ru.itclover.tsp.http.services.AkkaHttpUtils._

  implicit val vertexMetricsFormat = jsonFormat(
    VertexMetrics.apply,
    "read-records",
    "write-records",
    "currentEventTs"
  )
  implicit val vertexFormat = jsonFormat3(Vertex.apply)
  implicit object jobFormat extends RootJsonFormat[JobDetails] {
    override def write(obj: JobDetails): JsValue = JsObject(
      ("jid", JsString(obj.jid)),
      ("name", JsString(obj.name)),
      ("state", JsString(obj.state)),
      ("start-time", JsNumber(obj.startTsMs)),
      ("duration", JsNumber(obj.durationMs)),
      ("vertices", JsArray(obj.vertices.map(vertexFormat.write))),
      ("read-records", JsNumber(obj.readRecords)),
      ("write-records", JsNumber(obj.writeRecords))
    )

    override def read(json: JsValue): JobDetails = json match {
      case JsObject(fields) =>
        JobDetails(
          fields("jid").convertTo[String],
          fields("name").convertTo[String],
          fields("state").convertTo[String],
          fields("start-time").convertTo[Long],
          fields("duration").convertTo[Long],
          fields("vertices").convertTo[Vector[Vertex]]
        )
      case _ => throw new DeserializationException(s"Cannot deserialize $json as JobDetails")
    }
  }

  type TypedRequest = (QueueableRequest, String, String)
  implicit val requestOrdering: KeyOrder[TypedRequest] = (x: TypedRequest, y: TypedRequest) => -x._1.compare(y._1)
  implicit val requestSerialiser = new Serializer[TypedRequest] {
    override def write(data: (QueueableRequest, String, String)): Slice[Byte] = Slice
      .ofBytesScala(100000)
      .addStringUTF8WithSize(data._2)
      .addStringUTF8WithSize(data._3)
      .addAll(data._1.serialize)
      .close()

    override def read(slice: Slice[Byte]): (QueueableRequest, String, String) = {
      val reader = slice.createReader
      val in = reader.readStringWithSizeUTF8
      val out = reader.readStringWithSizeUTF8
      val request = QueueableRequest.deserialize(reader.readRemaining.toArray)
      (request, in, out)
    }
  }

  case class Metric(id: String, value: String)

  implicit val metricFmt = jsonFormat2(Metric.apply)

  private val log = Logger[QueueManagerService]

  val jobQueue = PersistentSet[TypedRequest, Nothing, Glass](dir = Paths.get("/tmp/job_queue"))
  log.warn(s"Recovering job queue: ${jobQueue.count} entries found")

  val isLocalhost: Boolean = uri.authority.host.toString match {
    case "localhost" | "127.0.0.1" | "::1" => true
    case _                                 => false
  }

  val ex = new ScheduledThreadPoolExecutor(1)

  val task: Runnable = new Runnable {
    def run(): Unit = onTimer()
  }
  val f: ScheduledFuture[_] = ex.scheduleAtFixedRate(task, 0, 4, TimeUnit.SECONDS)
  //f.cancel(false)

  def enqueue[
    In <: InputConf[_, _, _]: ClassTag,
    Out <: OutputConf[_]: ClassTag
  ](r: FindPatternsRequest[In, Out]): Unit = {
    jobQueue.add(
      (r,
        confClassTagToString(implicitly[ClassTag[In]]),
        confClassTagToString(implicitly[ClassTag[Out]])
      )
      )
  }

  def confClassTagToString(ct: ClassTag[_]): String = ct.runtimeClass match {
    case c if c.isAssignableFrom(classOf[JDBCInputConf]) => "from-jdbc"
    case c if c.isAssignableFrom(classOf[InfluxDBInputConf]) => "from-influxdb"
    case c if c.isAssignableFrom(classOf[KafkaInputConf]) => "from-kafka"
    case c if c.isAssignableFrom(classOf[JDBCOutputConf]) => "to-jdbc"
    case c if c.isAssignableFrom(classOf[KafkaOutputConf]) => "to-kafka"
    case _ => "unknown"
  }

  def getQueuedJobs: Seq[QueueableRequest] = jobQueue.asScala.map(_._1).toSeq

  def runJdbcToJdbc(request: FindPatternsRequest[JDBCInputConf, JDBCOutputConf]): Unit = {
    log.info("JDBC-to-JDBC: query started")
    import request._
    val fields: Set[Symbol] = PatternFieldExtractor.extract(patterns)
    log.info("JDBC-to-JDBC: extracted fields from patterns. Creating source...")
    val resultOrErr = for {
      source <- JdbcSource.create(inputConf, fields)
      _ = log.info("JDBC-to-JDBC: source created. Creating patterns stream...")
      _ <- createStream(patterns, inputConf, outConf, source)
      _ = log.info("JDBC-to-JDBC: stream created. Starting the stream...")
      result <- runStream(uuid)
      _ = log.info("JDBC-to-JDBC: stream started")
    } yield result
    resultOrErr match {
      case Left(error) => log.error(s"Cannot run request. Reason: $error")
      case Right(_)    => log.info(s"Stream successfully started!")
    }
  }

  def runJdbcToKafka(request: FindPatternsRequest[JDBCInputConf, KafkaOutputConf]): Unit = {
    import request._
    val fields: Set[Symbol] = PatternFieldExtractor.extract(patterns)

    val resultOrErr = for {
      source <- JdbcSource.create(inputConf, fields)
      _      <- createStream(patterns, inputConf, outConf, source)
      result <- runStream(uuid)
    } yield result
    resultOrErr match {
      case Left(error) => log.error(s"Cannot run request. Reason: $error")
      case Right(_)    => log.info(s"Stream successfully started!")
    }
  }

  def runKafkaToJdbc(request: FindPatternsRequest[KafkaInputConf, JDBCOutputConf]): Unit = {
    import request._
    val fields: Set[Symbol] = PatternFieldExtractor.extract(patterns)

    val resultOrErr = for {
      source <- KafkaSource.create(inputConf, fields)
      _ = log.info("Kafka create done")
      _ <- createStream(patterns, inputConf, outConf, source)
      _ = log.info("Kafka createStream done")
      result <- runStream(uuid)
      _ = log.info("Kafka runStream done")
    } yield result
    resultOrErr match {
      case Left(error) => log.error(s"Cannot run request. Reason: $error")
      case Right(_)    => log.info(s"Stream successfully started!")
    }
  }

  def runKafkaToKafka(request: FindPatternsRequest[KafkaInputConf, KafkaOutputConf]): Unit = {
    import request._
    val fields: Set[Symbol] = PatternFieldExtractor.extract(patterns)

    val resultOrErr = for {
      source <- KafkaSource.create(inputConf, fields)
      _ = log.info("Kafka create done")
      _ <- createStream(patterns, inputConf, outConf, source)
      _ = log.info("Kafka createStream done")
      result <- runStream(uuid)
      _ = log.info("Kafka runStream done")
    } yield result
    resultOrErr match {
      case Left(error) => log.error(s"Cannot run request. Reason: $error")
      case Right(_)    => log.info(s"Stream successfully started!")
    }
  }

  def runInfluxToJdbc(request: FindPatternsRequest[InfluxDBInputConf, JDBCOutputConf]): Unit = {
    import request._
    val fields: Set[Symbol] = PatternFieldExtractor.extract(patterns)

    val resultOrErr = for {
      source <- InfluxDBSource.create(inputConf, fields)
      _      <- createStream(patterns, inputConf, outConf, source)
      result <- runStream(uuid)
    } yield result
    resultOrErr match {
      case Left(error) => log.error(s"Cannot run request. Reason: $error")
      case Right(_)    => log.info(s"Stream successfully started!")
    }
  }

  def runInfluxToKafka(request: FindPatternsRequest[InfluxDBInputConf, KafkaOutputConf]): Unit = {
    import request._
    val fields: Set[Symbol] = PatternFieldExtractor.extract(patterns)

    val resultOrErr = for {
      source <- InfluxDBSource.create(inputConf, fields)
      _      <- createStream(patterns, inputConf, outConf, source)
      result <- runStream(uuid)
    } yield result
    resultOrErr match {
      case Left(error) => log.error(s"Cannot run request. Reason: $error")
      case Right(_)    => log.info(s"Stream successfully started!")
    }
  }

  /*def dequeueAndRun(slots: Int): Unit = {
    // TODO: Functional style
    var slotsRemaining = slots
    while (jobQueue.nonEmpty && slotsRemaining >= jobQueue.head._1.requiredSlots) {
      val request = jobQueue.dequeue()
      slotsRemaining -= request._1.requiredSlots
      run(request)
    }
  }*/

  def dequeueAndRunSingleJob(): Unit = {
    val request = jobQueue.head.get
    jobQueue.remove(request)
    run(request)
  }

  def removeFromQueue(uuid: String): Option[Unit] = {
    val job = jobQueue.asScala.find(_._1.uuid == uuid)
    job match {
      case Some(value) => {
        jobQueue.remove(value)
        Some(())
      }
      case None => None
    }
  }

  def queueAsScalaSeq: Seq[QueueableRequest] = jobQueue.asScala.map(_._1).toSeq

  def run(typedRequest: TypedRequest): Unit = {
    val (request, inClass, outClass) = typedRequest
    log.info(s"Dequeued job ${request.uuid}, sending")
    (inClass, outClass) match {
      case ("from-jdbc", "to-jdbc") =>
        runJdbcToJdbc(request.asInstanceOf[FindPatternsRequest[JDBCInputConf, JDBCOutputConf]])
      case ("from-jdbc", "to-kafka") =>
        runJdbcToKafka(request.asInstanceOf[FindPatternsRequest[JDBCInputConf, KafkaOutputConf]])
      case ("from-kafka", "to-jdbc") =>
        runKafkaToJdbc(request.asInstanceOf[FindPatternsRequest[KafkaInputConf, JDBCOutputConf]])
      case ("from-kafka", "to-jdbc") =>
        runKafkaToKafka(request.asInstanceOf[FindPatternsRequest[KafkaInputConf, KafkaOutputConf]])
      case ("from-influxdb", "to-jdbc") =>
        runInfluxToJdbc(request.asInstanceOf[FindPatternsRequest[InfluxDBInputConf, JDBCOutputConf]])
      case ("from-influxdb", "to-jdbc") =>
        runInfluxToKafka(request.asInstanceOf[FindPatternsRequest[InfluxDBInputConf, KafkaOutputConf]])
      case _ =>
        log.error(s"Unknown job request type: IN: $inClass --- OUT: $outClass")
    }
  }

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

  def runStream(uuid: String): Either[RuntimeErr, Option[JobExecutionResult]] = {
    log.debug("runStream started")

    // Just detach job thread
    val res = Future {
      reporting match {
        case Some(value) =>
          streamEnv.registerJobListener(
            StatusReporter(uuid, value.brokers, value.topic, this)
          )
        case None =>
          streamEnv.registerJobListener(
            ConsoleStatusReporter(uuid, this)
          )
      }
      streamEnv.execute(uuid)
    }(blockingExecutionContext)
    Try(Await.ready(res, Duration.create(1, SECONDS)))

    log.debug("runStream finished")
    Right(None)
  }

  def availableSlots: Future[Int] = if (isLocalhost) Future(32)
  else
    Http()
      .singleRequest(HttpRequest(uri = uri.toString + "/jobmanager/metrics?get=taskSlotsAvailable"))
      .flatMap(resp => Unmarshal(resp).to[Seq[Metric]])
      .map(m => Try(m.head.value.toInt).getOrElse(0))

  def getJobNameByID(id: JobID): Option[String] = {
    val res: Future[String] = Http()
      .singleRequest(HttpRequest(uri = uri.toString + s"/jobs/${id}/"))
      .flatMap(resp => Unmarshal(resp).to[JobDetails])
      .map(det => det.name)
    Try(Await.result(res, Duration.create(500, MILLISECONDS))).toOption
  }

  def onTimer(): Unit = {
    availableSlots.onComplete {
      case Success(slots) =>
        if (slots > 0 && jobQueue.nonEmpty) {
          log.info(s"$slots slots available")
          dequeueAndRunSingleJob()
        } else {
          if (jobQueue.nonEmpty)
            log.info(
              s"Waiting for free slot ($slots available), cannot run jobs right now"
            )
        }
      case Failure(exception) =>
        log.warn(s"An exception occurred when checking available slots: $exception --- ${exception.getMessage}")
        log.warn("Trying to send job anyway (assuming 1 available slot)...")
        dequeueAndRunSingleJob()
    }

  }
}

object QueueManagerService {
  val services: mutable.Map[Uri, QueueManagerService] = mutable.Map.empty

  def getOrCreate(uri: Uri, blockingExecutionContext: ExecutionContextExecutor)(
    implicit executionContext: ExecutionContextExecutor,
    streamEnv: StreamExecutionEnvironment,
    actorSystem: ActorSystem,
    materializer: ActorMaterializer,
    decoders: BasicDecoders[Any] = AnyDecodersInstances,
    reporting: Option[JobReporting],
    typeInfoRowWithIdx: TypeInformation[RowWithIdx],
    typeInfoRow: TypeInformation[Row]
  ): QueueManagerService = {
    if (!services.contains(uri)) services(uri) = new QueueManagerService(uri, blockingExecutionContext)
    services(uri)
  }
}
