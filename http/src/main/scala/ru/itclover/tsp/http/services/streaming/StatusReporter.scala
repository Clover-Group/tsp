package ru.itclover.tsp.http.services.streaming

import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.scalalogging.Logger
import org.apache.flink.api.common.{JobExecutionResult, JobID}
import org.apache.flink.core.execution.{JobClient, JobListener}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.Serializer
import ru.itclover.tsp.http.services.queuing.QueueManagerService

import java.time.LocalDateTime
import collection.JavaConverters._
import scala.beans.BeanProperty
import scala.collection.mutable
import scala.concurrent.ExecutionContextExecutor
import scala.util.{Failure, Success, Try}

case class StatusMessage(
  @BeanProperty uuid: String,
  @BeanProperty timestamp: String,
  @BeanProperty status: String,
  @BeanProperty flinkStatus: String,
  @BeanProperty text: String
)

class StatusMessageSerializer extends Serializer[StatusMessage] {
  private val objectMapper = new ObjectMapper()

  override def serialize(topic: String, data: StatusMessage): Array[Byte] =
    objectMapper.writeValueAsBytes(data)
}

case class StatusReporter(jobName: String, brokers: String, topic: String, queueManagerService: QueueManagerService)
                         (implicit executionEnvironment: StreamExecutionEnvironment,
                                   executionContext: ExecutionContextExecutor)
  extends JobListener {

  val config: Map[String, Object] = Map(
    "bootstrap.servers" -> brokers,
    "key.serializer"    -> "org.apache.kafka.common.serialization.StringSerializer",
    "value.serializer"  -> "ru.itclover.tsp.http.services.streaming.StatusMessageSerializer"
  )

  val messageProducer = new KafkaProducer[String, StatusMessage](config.asJava)

  var client: Option[JobClient] = None

  val log = Logger[StatusReporter]


  override def onJobSubmitted(jobClient: JobClient, throwable: Throwable): Unit = {
    val jobNameOption = queueManagerService.getJobNameByID(jobClient.getJobID)
    if (!jobNameOption.contains(jobName)) {
      log.warn(s"Wrong job name $jobNameOption for job ID ${jobClient.getJobID}, skipping")
      return
    }

    if (jobClient != null && client.isEmpty) {
      client = Some(jobClient)
      val now = LocalDateTime.now.toString
      val record = new ProducerRecord[String, StatusMessage](
        topic,
        now,
        StatusMessage(
          jobName,
          now,
          "SUBMITTED",
          Try(jobClient.getJobStatus.get().name).toOption.getOrElse("no status"),
          client match {
            case Some(value) => s"Job submitted with id ${value.getJobID}"
            case None        => s"Job submission failed"
          }
        )
      )
      messageProducer.send(record)
      messageProducer.flush()
    }
  }

  def unregisterSelf(): Unit = {
    executionEnvironment.getJavaEnv.getJobListeners.remove(this)
  }

  override def onJobExecuted(jobExecutionResult: JobExecutionResult, throwable: Throwable): Unit = {
    client.foreach { c =>
      if (jobExecutionResult != null && c.getJobID.toHexString != jobExecutionResult.getJobID.toHexString) {
        return
      }
      val now = LocalDateTime.now.toString
      val status = Try(c.getJobStatus.get().name).getOrElse("status unknown")
      val record = new ProducerRecord[String, StatusMessage](
        topic,
        now,
        StatusMessage(
          jobName,
          now,
          throwable match {
            case null => "FINISHED"
            case _    => "FAILED"
          },
          status,
          throwable match {
            case null =>
              s"Job executed with no exceptions in ${jobExecutionResult.getNetRuntime} ms"
            case _    => s"Job executed with exception: ${throwable.getStackTrace.mkString("\n")}"
          }
        )
      )
      // Unregister
      if (throwable == null) unregisterSelf()
      messageProducer.send(record)
      messageProducer.flush()
    }
  }
}
