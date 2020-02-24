package ru.itclover.tsp.http.services.flink

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.unmarshalling.Unmarshaller
import ru.itclover.tsp.mappers.PatternProcessor
import spray.json.DefaultJsonProtocol


object MonitoringServiceModel {

  /** @param metrics - set of metrics names and values */
  case class JobDetailsWithMetrics(details: JobDetails, metrics: Map[String, String])

  case class JobDetails(
    jid: String,
    name: String,
    state: String,
    startTsMs: Long,
    durationMs: Long,
    vertices: Vector[Vertex]
  )

  case class Metric(id: String, value: String)

  case class MetricName(id: String)

  case class MetricInfo(vertexIndex: Int, id: String, name: String)

  object MetricInfo {
    def onLastVertex(id: String, name: String) = MetricInfo(Int.MaxValue, id, name)
  }

  case class Vertex(id: String, name: String, metrics: VertexMetrics)

  case class VertexMetrics(readRecords: Long, writeRecords: Long, currentEventTs: Option[Long])

  case class JobsOverview(jobs: List[JobBrief])

  case class JobBrief(jid: String, name: String)

  case class JobExceptions(timestamp: Long, rootException: String, truncated: Boolean)

  case class EmptyResponse()

  case class MonitoringException(err: String) extends RuntimeException(err)

  case class MonitoringError(errors: Seq[String]) {
    def toThrowable = MonitoringException(errors.mkString("; "))
  }

}

trait MonitoringServiceProtocols extends SprayJsonSupport with DefaultJsonProtocol {
  import MonitoringServiceModel._

  implicit val jobExceptionsFormat = jsonFormat(JobExceptions.apply, "timestamp", "root-exception", "truncated")
  implicit val emptyFormat = jsonFormat0(EmptyResponse.apply _)
  implicit val metricFormat = jsonFormat2(Metric.apply)
  implicit val metricNameFormat = jsonFormat1(MetricName.apply)
  implicit val monitoringErrorFormat = jsonFormat1(MonitoringError.apply)
  implicit val vertexMetricsFormat = jsonFormat(
    VertexMetrics.apply,
    "read-records",
    "write-records",
    PatternProcessor.currentEventTsMetric
  )

  implicit val vertexFormat = jsonFormat3(Vertex.apply)
  implicit val jobFormat = jsonFormat(JobDetails.apply, "jid", "name", "state", "start-time", "duration", "vertices")
  implicit val jobBriefFormat = jsonFormat2(JobBrief.apply)
  implicit val jobInfoFormat = jsonFormat3(MetricInfo.apply)
  implicit val jobOverviewFormat = jsonFormat1(JobsOverview.apply)
  implicit val jobDetailsAndMetricsFormat = jsonFormat2(JobDetailsWithMetrics.apply)

  implicit val errorOrDetailsUnmarshaller = Unmarshaller.eitherUnmarshaller[MonitoringError, JobDetails]
  implicit val errorOrInfoUnmarshaller = Unmarshaller.eitherUnmarshaller[MonitoringError, JobsOverview]
  implicit val errorOrUnitUnmarshaller = Unmarshaller.eitherUnmarshaller[MonitoringError, EmptyResponse]
  implicit val errorOrMetricUnmarshaller = Unmarshaller.eitherUnmarshaller[MonitoringError, Metric]
  implicit val emptyOrDetailsUnmarshaller = Unmarshaller.eitherUnmarshaller[EmptyResponse, JobDetails]
}
