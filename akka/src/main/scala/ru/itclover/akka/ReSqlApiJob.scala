package ru.itclover.akka

import java.util.concurrent.Executors
import javax.sql.DataSource

import akka.Done
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscription, Subscriptions}
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.codahale.metrics.SharedMetricRegistries
import com.evolutiongaming.analytics.realtime.DwhGraphiteReporter
import com.evolutiongaming.analytics.realtime.KafkaSource.KafkaSourceConfig
import com.evolutiongaming.analytics.realtime.common.HikariConfigOps.HikariConfigImplicits
import com.evolutiongaming.analytics.realtime.common.JdbcConfig
import com.evolutiongaming.analytics.resql.ReSqlApiJob.ReSqlApiConfig
import com.evolutiongaming.analytics.resql.calcite.BufferTable
import com.evolutiongaming.analytics.resql.cleaner.KafkaUtils.OffsetReceiverConfig
import com.evolutiongaming.analytics.resql.cleaner.{KafkaUtils, OffsetReceiverActor}
import com.evolutiongaming.analytics.resql.config.TableDefinition
import com.evolutiongaming.analytics.resql.master.MasterOffsetCommitterActor
import com.evolutiongaming.analytics.resql.slave.SlaveOffsetHandlerActor
import com.evolutiongaming.analytics.resql.stage.BufferBackPressure
import com.github.andr83.scalaconfig.Reader
import com.typesafe.scalalogging.LazyLogging
import com.zaxxer.hikari.HikariConfig
import io.parsek.jackson.JsonSerDe
import nl.grons.metrics.scala.DefaultInstrumented
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

/**
 * @author andr83
 */
final class ReSqlApiJob(config: ReSqlApiConfig, tables: Seq[TableDefinition])(implicit system: ActorSystem,
                                                                              materializer: Materializer)
    extends DwhGraphiteReporter
    with LazyLogging
    with DefaultInstrumented {

  import ReSqlApiJob._

  private val jsonErrorCounter = this.metrics.counter("jsonDecodeErrors")

  def start(): QueryActorRef = {
    val ioEC = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())

    val clickhouseDbIO = createClickhouseDbIO(ioEC)
    val calciteDbIO = createCalciteDbIO(clickhouseDbIO.dataSource)(ioEC)

    tables foreach (tableDef => {
      config.role match {
        case Role.Master => startMaster(tableDef, clickhouseDbIO, calciteDbIO)
        case Role.Slave => startSlave(tableDef, clickhouseDbIO, calciteDbIO)
      }
    })

    system.actorOf(QueryActor.props(calciteDbIO, clickhouseDbIO))
  }

  private def createKafkaStream(table: BufferTable,
                                name: String,
                                messageProcessor: CommittableMessageProcessor,
                                subscription: Subscription,
                                attempt: Int,
                                recoverDelay: FiniteDuration): Future[Done] = {
    import system.dispatcher

    logger.info(s"Start Kafka. Attempt No. $attempt")

    val consumerSettings = ConsumerSettings(system, new ByteArrayDeserializer, new StringDeserializer)
      .withBootstrapServers(config.kafka.brokers)
      .withGroupId(s"${config.kafka.groupId}.$name")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.kafka.offsetReset.getOrElse("latest"))

    val kafkaFuture = Consumer
      .committableSource(consumerSettings, subscription)
      .via(new BufferBackPressure(() => table.buffer.size(), config.buffer))
      .map(messageProcessor.process)
      .runWith(Sink.ignore)

    kafkaFuture onComplete {
      case Success(_) => logger.info("Kafka stream has finished")
      case Failure(NonFatal(ex)) =>
        logger.error(s"Kafka stream died with error: $ex")
        logger.info(s"Will recover Kafka stream in $recoverDelay")
        akka.pattern.after(recoverDelay, system.scheduler)(
          createKafkaStream(table, name, messageProcessor, subscription, attempt + 1, recoverDelay)
        )
      case Failure(ex) =>
        logger.error(s"Kafka stream died with error: $ex")
        sys.exit(1)
    }

    kafkaFuture
  }

  private def createClickhouseDbIO(implicit ec: ExecutionContext): ClickhouseDbIO = {
    val dataSource = {
      logger.debug(s"Connecting to Clickhouse: ${config.clickhouse.jdbcUrl}")
      try {
        new HikariConfig().createDataSource(config.clickhouse, SharedMetricRegistries.getOrCreate("default"))
      } catch {
        case e: Throwable =>
          logger.error(e.getMessage, e)
          sys.exit(1)
      }
    }
    ClickhouseDbIO(dataSource, config.metricsEnabled)(ec)
  }

  def createCalciteDbIO(dataSource: DataSource)(implicit ec: ExecutionContext): CalciteDbIO = {
    val bufferTables = tables.map(tableDef => {
      new BufferTable(dataSource, tableDef.schema, tableDef.pkFields.toArray, tableDef.tableName)
    })
    CalciteDbIO(bufferTables, dataSource, config.metricsEnabled)(ec)
  }

  private def startMaster(tableDef: TableDefinition, clickhouseDbIO: ClickhouseDbIO, calciteDbIO: CalciteDbIO): Unit = {
    startStream(
      Role.Master,
      tableDef,
      clickhouseDbIO,
      calciteDbIO,
      calciteBuffer =>
        tp =>
          MasterOffsetCommitterActor
            .props(tp.topic(), tp.partition(), calciteBuffer, system.scheduler, config.tickInterval),
      _ => Subscriptions.topics(config.kafka.topics)
    )
  }

  private def startSlave(tableDef: TableDefinition, clickhouseDbIO: ClickhouseDbIO, calciteDbIO: CalciteDbIO): Unit = {
    val subscriptionCreator: OffsetActorRef => Subscription = offsetActorRef => {
      val receiverConfig = OffsetReceiverConfig(
        config.kafka.brokers,
        config.masterGroupId
          .map(gId => s"$gId.${tableDef.tableName}")
          .getOrElse(throw new IllegalStateException("For slave role masterGroupId must be defined")),
        config.kafka.topics
      )
      val offsetReceiver = KafkaUtils.getOffsetReceiver(receiverConfig)
      system.actorOf(OffsetReceiverActor.props(offsetActorRef, offsetReceiver, config.tickInterval))
      Subscriptions.assignmentWithOffset(offsetReceiver.getOffsets())
    }

    startStream(
      Role.Slave,
      tableDef,
      clickhouseDbIO,
      calciteDbIO,
      calciteBuffer => tp => SlaveOffsetHandlerActor.props(tp.topic(), tp.partition(), calciteBuffer),
      subscriptionCreator
    )
  }

  private def startStream(role: Role,
                          tableDef: TableDefinition,
                          clickhouseDbIO: ClickhouseDbIO,
                          calciteDbIO: CalciteDbIO,
                          offsetActorCreator: CalciteBufferActorRef => TopicPartition => Props,
                          subscriptionCreator: OffsetActorRef => Subscription): Future[Done] = {
    val bufferTable = calciteDbIO.getTable(tableDef.tableName)
    val clickhouseStorage = ClickhouseStorageImpl(clickhouseDbIO, bufferTable)
    val calciteStorage = CalciteBufferTableStorageImpl(calciteDbIO, bufferTable)

    val calciteBuffer = system.actorOf(
      CalciteBufferActor.props(calciteStorage, clickhouseStorage),
      s"calciteBuffer.${tableDef.tableName}"
    )
    val offsetActorRef = system.actorOf(
      Props(new OffsetActor(offsetActorCreator(calciteBuffer))),
      s"${role}OffsetActor.${tableDef.tableName}"
    )

    val eventsParser = new EventsParser(JsonSerDe(), tableDef.schema, tableDef.converter, jsonErrorCounter)
    val messageProcessor = new CommittableMessageProcessor(eventsParser, calciteBuffer, offsetActorRef)

    createKafkaStream(
      bufferTable,
      tableDef.tableName,
      messageProcessor,
      subscriptionCreator(offsetActorRef),
      attempt = 1,
      recoverDelay = config.kafka.recoverDelay
    )
  }
}

object ReSqlApiJob {
  type Topic = String
  type Partition = Int
  type CalciteBufferActorRef = ActorRef
  type OffsetActorRef = ActorRef
  type QueryActorRef = ActorRef

  sealed trait Role

  object Role {

    implicit val roleReader: Reader[Role] = Reader.pureV[Role] { (config, path) =>
      config.getString(path) match {
        case "master" => Right(Role.Master)
        case "slave" => Right(Role.Slave)
        case other => Left(Seq(new IllegalStateException(s"Expected role master or slave but got $other")))
      }
    }

    final case object Master extends Role

    final case object Slave extends Role

  }

  case class BufferConfig(maxSize: Int = 100000, // buffer max size
                          checkFrequency: Int = 1000, // how often check buffer size: every checkFrequency messages
                          retryFrequency: FiniteDuration = 1.second // if buffer is full how often do check retries
  )

  case class ReSqlApiConfig(role: Role, //master or slave
                            kafka: KafkaSourceConfig,
                            clickhouse: JdbcConfig,
                            buffer: BufferConfig = BufferConfig(),
                            masterGroupId: Option[String] = None,
                            metricsEnabled: Boolean = false,
                            tickInterval: FiniteDuration = 10.seconds)

}
