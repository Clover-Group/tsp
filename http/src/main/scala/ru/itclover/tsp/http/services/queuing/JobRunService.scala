package ru.itclover.tsp.http.services.queuing

import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.Uri
import akka.stream.Materializer
import cats.effect.{IO, Resource}
import cats.effect.unsafe.implicits.global
import cats.implicits.toTraverseOps
import com.typesafe.scalalogging.Logger
import fs2.concurrent.SignallingRef
import ru.itclover.tsp.StreamSource.Row
import ru.itclover.tsp.core.{Incident, RawPattern}
import ru.itclover.tsp.{JdbcSource, KafkaSource, RowWithIdx, StreamSource}
import ru.itclover.tsp.core.io.{AnyDecodersInstances, BasicDecoders}
import ru.itclover.tsp.dsl.PatternFieldExtractor
import ru.itclover.tsp.http.domain.input.{FindPatternsRequest, QueueableRequest}
import ru.itclover.tsp.http.services.coordinator.CoordinatorService
import ru.itclover.tsp.streaming.io.{InputConf, JDBCInputConf, KafkaInputConf}
import ru.itclover.tsp.streaming.io.OutputConf
import ru.itclover.tsp.streaming.mappers.PatternsToRowMapper
import ru.itclover.tsp.streaming.PatternsSearchJob
import ru.itclover.tsp.streaming.checkpointing.CheckpointingService
import ru.itclover.tsp.streaming.utils.ErrorsADT
import ru.itclover.tsp.streaming.utils.ErrorsADT.RuntimeErr
import spray.json._

import java.util.concurrent.{ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}
import scala.collection.mutable
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.reflect.ClassTag
import scala.util.{Failure, Success}
import cats.effect.kernel.Deferred
import scala.concurrent.duration.FiniteDuration
import java.util.concurrent.atomic.AtomicBoolean

class JobRunService(id: String, blockingExecutionContext: ExecutionContextExecutor)(implicit
  executionContext: ExecutionContextExecutor,
  actorSystem: ActorSystem,
  materializer: Materializer,
  decoders: BasicDecoders[Any] = AnyDecodersInstances
) extends SprayJsonSupport
    with DefaultJsonProtocol {

  type TypedRequest = (QueueableRequest, String)
  type Request = FindPatternsRequest[RowWithIdx, String, Any, Row]

  case class Metric(id: String, value: String)

  implicit val metricFmt: RootJsonFormat[Metric] = jsonFormat2(Metric.apply)

  private val log = Logger[JobRunService]

  // val jobQueue = PersistentSet[TypedRequest, Nothing, Glass](dir = Paths.get("/tmp/job_queue"))
  // log.warn(s"Recovering job queue: ${jobQueue.count} entries found")
  val jobQueue = mutable.Queue[TypedRequest]()

  val runningStreams = mutable.Map[String, AtomicBoolean]()

  val runningJobsRequests = mutable.Map[String, QueueableRequest]()

  val isLocalhost: Boolean = true

  val ex = new ScheduledThreadPoolExecutor(1)

  val task: Runnable = new Runnable {
    def run(): Unit = onTimer()
  }

  val f: ScheduledFuture[_] = ex.scheduleAtFixedRate(task, 0, 1, TimeUnit.SECONDS)
  // f.cancel(false)

  def enqueue(r: Request): Unit = {
    jobQueue.enqueue(
      (r, confClassTagToString(ClassTag(r.inputConf.getClass)))
    )
    log.info(s"Job ${r.uuid} enqueued.")
  }

  def confClassTagToString(ct: ClassTag[_]): String = ct.runtimeClass match {
    case c if c.isAssignableFrom(classOf[JDBCInputConf])  => "from-jdbc"
    case c if c.isAssignableFrom(classOf[KafkaInputConf]) => "from-kafka"
    case _                                                => "unknown"
  }

  def getQueuedJobs: Seq[QueueableRequest] = jobQueue.map(_._1).toSeq

  def runJdbc(request: Request): Unit = {
    log.info("JDBC-to-JDBC: query started")
    import request._
    val fields: Set[String] = PatternFieldExtractor.extract(patterns)
    log.info("JDBC-to-JDBC: extracted fields from patterns. Creating source...")
    val resultOrErr = for {
      source <- JdbcSource.create(inputConf.asInstanceOf[JDBCInputConf], fields)
      _ = log.info("JDBC-to-JDBC: source created. Creating patterns stream...")
      streams <- createStream(request.uuid, patterns, inputConf, outConf, source)
      _ = log.info("JDBC-to-JDBC: stream created. Starting the stream...")
      result <- runStream(uuid, streams)
      _ = log.info("JDBC-to-JDBC: stream started")
    } yield result
    resultOrErr match {
      case Left(error) =>
        log.error(s"Cannot run request. Reason: $error")
        CoordinatorService.notifyJobCompleted(uuid, Some(new Exception(error.toString)))
      case Right(_) => log.info(s"Stream successfully started!")
    }
  }

  def runKafka(request: Request): Unit = {
    import request._
    val fields: Set[String] = PatternFieldExtractor.extract(patterns)

    val resultOrErr = for {
      source <- KafkaSource.create(inputConf.asInstanceOf[KafkaInputConf], fields)
      _ = log.info("Kafka create done")
      streams <- createStream(request.uuid, patterns, inputConf, outConf, source)
      _ = log.info("Kafka createStream done")
      result <- runStream(uuid, streams)
      _ = log.info("Kafka runStream done")
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
    log.info(s"Running job ${jobQueue.head._1.uuid}")
    val request = jobQueue.head
    jobQueue.dequeue()
    run(request)
  }

  def removeFromQueue(uuid: String): Option[Unit] = {
    val job = jobQueue.find(_._1.uuid == uuid)
    job match {
      case Some(_) => {
        // jobQueue.remove(value)
        Some(())
      }
      case None => None
    }
  }

  def queueAsScalaSeq: Seq[QueueableRequest] = jobQueue.map(_._1).toSeq

  def run(typedRequest: TypedRequest): Unit = {
    val (r, inClass) = typedRequest
    val request = r.asInstanceOf[Request]
    log.info(s"Dequeued job ${request.uuid}, sending")
    inClass match {
      case "from-jdbc" =>
        runJdbc(request)
        runningJobsRequests(request.uuid) = request
      case "from-kafka" =>
        runKafka(request)
        runningJobsRequests(request.uuid) = request
      case _ =>
        log.error(s"Unknown job request type: IN: $inClass")
    }
  }

  type EKey = String

  def createStream[E, EItem](
    uuid: String,
    patterns: Seq[RawPattern],
    inputConf: InputConf[E, EKey, EItem],
    outConf: Seq[OutputConf[Row]],
    source: StreamSource[E, EKey, EItem]
  )(implicit decoders: BasicDecoders[EItem]): Either[ErrorsADT.ConfigErr, Resource[IO, fs2.Stream[IO, Unit]]] = {

    log.debug("createStream started")

    val searcher = PatternsSearchJob(uuid, source, decoders)
    val strOrErr = searcher.patternsSearchStream(
      patterns,
      outConf,
      outConf.map(conf => PatternsToRowMapper[Incident, Row](conf.rowSchema))
    )
    strOrErr.map { case (parsedPatterns, stream) =>
      // .. patternV2.format
      val strPatterns = parsedPatterns.map { case ((_, meta), _) =>
        /*p.format(source.emptyEvent) +*/
        s" ;; Meta=$meta"
      }
      log.debug(s"Parsed patterns:\n${strPatterns.mkString(";\n")}")
      stream
    }
  }

  def runStream(uuid: String, stream: Resource[IO, fs2.Stream[IO, Unit]]): Either[RuntimeErr, Option[String]] = {
    log.debug("runStream started")
    CoordinatorService.notifyJobStarted(uuid)

    // Run the stream
    Deferred[IO, Either[Throwable, Unit]]
      .flatMap { cancelToken =>
        runningStreams(uuid) = new AtomicBoolean(true)

        val f = Future {
          while (runningStreams.get(uuid).map(_.get).getOrElse(false)) {
            log.info(s"Stream $uuid running")
            Thread.sleep(5000)
          }
        }

        val cancel = IO.async { cb =>
          f.onComplete(t => cb(t.toEither))
          IO.pure(Some(IO.unit))
        }
          >> {
            log.info(s"Stream $uuid finished")
            cancelToken.complete(Right(()))
          }

        val program = stream.use {
          _.interruptWhen(cancelToken).compile.drain
        }

        log.debug("runStream finished")

        cancel.background.surround(program)

      }
      .unsafeRunAsync {
        case Left(throwable) =>
          log.error(s"Job $uuid failed: $throwable")
          CoordinatorService.notifyJobCompleted(uuid, Some(throwable))
          CheckpointingService.removeCheckpointAndState(uuid)
          runningStreams.remove(uuid)
          runningJobsRequests.remove(uuid)
        case Right(_) =>
          // success
          log.info(s"Job $uuid finished")
          CoordinatorService.notifyJobCompleted(uuid, None)
          CheckpointingService.removeCheckpointAndState(uuid)
          runningStreams.remove(uuid)
          runningJobsRequests.remove(uuid)
      }

    Right(None)
  }

  def stopStream(uuid: String): Unit = runningStreams.get(uuid).map { _ =>
    log.info(s"Job $uuid stopped")
    runningStreams(uuid).set(false)
  }

  def getRunningJobsIds: Seq[String] = runningStreams.keys.toSeq

  def onTimer(): Unit = {
    if (jobQueue.nonEmpty) {
      dequeueAndRunSingleJob()
    }
  }

}

object JobRunService {
  val services: mutable.Map[Uri, JobRunService] = mutable.Map.empty

  def getOrCreate(id: String, blockingExecutionContext: ExecutionContextExecutor)(implicit
    executionContext: ExecutionContextExecutor,
    actorSystem: ActorSystem,
    materializer: Materializer,
    decoders: BasicDecoders[Any] = AnyDecodersInstances
  ): JobRunService = {
    if (!services.contains(id)) services(id) = new JobRunService(id, blockingExecutionContext)
    services(id)
  }

}
