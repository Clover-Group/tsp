package ru.itclover.tsp.http.services.streaming

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.apache.spark.sql.SparkSession
import ru.itclover.tsp.http.services.streaming.MonitoringServiceModel._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

case class SparkMonitoringService(spark: SparkSession)(
  implicit as: ActorSystem,
  am: ActorMaterializer,
  ec: ExecutionContext
) extends MonitoringServiceProtocols {

  def queryJobInfo(name: String): Future[Option[JobDetails]] = Future { Try {
    val jobIds = spark.sparkContext.statusTracker.getJobIdsForGroup(name)
    val stageInfos = jobIds.map(jid => spark.sparkContext.statusTracker.getJobInfo(jid).get.stageIds()).reduce(_ ++ _)
      .map(sid => spark.sparkContext.statusTracker.getStageInfo(sid).get)
    val vertices = stageInfos.map(si => Vertex(si.stageId.toString, si.name, VertexMetrics(0, 0, None)))
    val statuses = jobIds.map(jid => spark.sparkContext.statusTracker.getJobInfo(jid).get.status().toString)
    val state = statuses match {
      case s if s.contains("FAILED") => "FAILED"
      case s if s.contains("UNKNOWN") => "UNKNOWN"
      case s if s.contains("RUNNING") => "RUNNING"
      case _ => "SUCCEEDED"
    }
    JobDetails(name, name, state, 0, 0, vertices.toVector)
  }.toOption
  }

  def queryJobExceptions(name: String): Future[Option[JobExceptions]] = Future { None }

  def sendStopQuery(jobName: String): Future[Option[Unit]] = Future {
    Try(spark.sparkContext.cancelJobGroup(jobName)).toOption
  }

  def queryJobsOverview: Future[JobsOverview] =  Future {
    JobsOverview(spark.sparkContext.statusTracker.getActiveJobIds.map(
      jid => JobBrief(jid.toString, jid.toString)
    ).toList)
  }


}
