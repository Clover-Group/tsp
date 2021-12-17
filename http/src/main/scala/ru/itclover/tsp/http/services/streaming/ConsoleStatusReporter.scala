package ru.itclover.tsp.http.services.streaming

import com.typesafe.scalalogging.Logger
import org.apache.flink.api.common.{JobExecutionResult, JobID}
import org.apache.flink.core.execution.{JobClient, JobListener}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.environment.{StreamExecutionEnvironment => JavaEnv}


import scala.util.Try

case class ConsoleStatusReporter(jobName: String)
                                (implicit executionEnvironment: StreamExecutionEnvironment)
  extends JobListener {


  var client: Option[JobClient] = None

  val log = Logger[ConsoleStatusReporter]

  override def onJobSubmitted(jobClient: JobClient, throwable: Throwable): Unit = {
    if (jobClient != null) client = Some(jobClient)
    val msg = StatusMessage(
      jobName,
      "SUBMITTED",
      Try(jobClient.getJobStatus.get().name).toOption.getOrElse("no status"),
      client match {
        case Some(value) => s"Job submitted with id ${value.getJobID}"
        case None        => s"Job submission failed"
      }
    )
    log.info(f"Job ${msg.uuid}: status=${msg.status}, Flink status=${msg.flinkStatus}, message=${msg.text}")
  }

  def unregisterSelf(): Unit = {
    executionEnvironment.getJavaEnv.getJobListeners.remove(this)
  }

  override def onJobExecuted(jobExecutionResult: JobExecutionResult, throwable: Throwable): Unit = {
    client.foreach { c =>
      if (jobExecutionResult != null && c.getJobID.toHexString != jobExecutionResult.getJobID.toHexString) {
        return
      }
      val status = Try(c.getJobStatus.get().name).getOrElse("status unknown")
      val msg = StatusMessage(
        jobName,
        throwable match {
          case null => "FINISHED"
          case _    => "FAILED"
        },
        status,
        throwable match {
          case null =>
            s"Job executed with no exceptions in ${jobExecutionResult.getNetRuntime} ms"
          case _    =>
            s"Job executed with exception: ${throwable.getStackTrace.mkString("\n")}"
        }
      )
      // Unregister
      unregisterSelf()
      log.info(f"Job ${msg.uuid}: status=${msg.status}, Flink status=${msg.flinkStatus}, message=${msg.text}")
    }
  }
}
