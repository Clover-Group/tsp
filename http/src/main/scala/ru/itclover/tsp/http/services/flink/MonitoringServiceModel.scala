package ru.itclover.tsp.http.services.flink

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json.{DefaultJsonProtocol, _}
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.{Unmarshal, Unmarshaller}
import scala.language.implicitConversions

object MonitoringServiceModel {

  case class JobDetails(jid: String, name: String, state: String, startTsMs: Long, durationMs: Long,
                        vertices: Vector[Vertex]) {
    def getNumProcessedRecords: Option[Int] = vertices.lastOption.map(_.metrics.readRecords)
    def getNumRecordsRead: Option[Int] = vertices.headOption.map(_.metrics.readRecords)
  }

  case class Vertex(id: String, name: String, metrics: VertexMetrics)

  case class VertexMetrics(readRecords: Int, writeRecords: Int)

  case class JobsOverview(jobs: List[JobInfo])

  case class JobInfo(jid: String, name: String)

  case class JobExceptions(timestamp: Long, rootException: String, truncated: Boolean)

  case class EmptyResponse()


  case class MonitoringException(err: String) extends RuntimeException(err)

  case class MonitoringError(errors: Seq[String]) {
    def toThrowable = MonitoringException(errors.mkString("; "))
  }

}

trait MonitoringServiceProtocols extends SprayJsonSupport with DefaultJsonProtocol {
  import MonitoringServiceModel._

  /** Add numProcessedRecords here to response. */
  implicit object JobDetailsFormat extends RootJsonFormat[JobDetails] {
    val jobFormat = jsonFormat(JobDetails.apply, "jid", "name", "state", "start-time", "duration", "vertices")

    override def read(json: JsValue): JobDetails =
      jobFormat.read(json)

    override def write(details: JobDetails): JsValue = {
      val json = jobFormat.write(details).asJsObject
      details.getNumProcessedRecords match {
        case Some(processed) => JsObject(json.fields + ("numProcessedRecords" -> JsNumber(processed)))
        case None => json
      }
    }
  }

  implicit val jobExceptionsFormat = jsonFormat(JobExceptions.apply, "timestamp", "root-exception", "truncated")
  implicit val emptyFormat = jsonFormat0(EmptyResponse.apply)
  implicit val monitoringErrorFormat = jsonFormat1(MonitoringError.apply)
  implicit val vertexMetricsFormat = jsonFormat(VertexMetrics.apply, "read-records", "write-records")
  implicit val vertexFormat = jsonFormat3(Vertex.apply)
  implicit val jobInfoFormat = jsonFormat2(JobInfo.apply)
  implicit val jobOverviewFormat = jsonFormat1(JobsOverview.apply)

  implicit val errorOrDetailsUnmarshaller = Unmarshaller.eitherUnmarshaller[MonitoringError, JobDetails]
  implicit val errorOrInfoUnmarshaller = Unmarshaller.eitherUnmarshaller[MonitoringError, JobsOverview]
  implicit val errorOrUnitUnmarshaller = Unmarshaller.eitherUnmarshaller[MonitoringError, EmptyResponse]
  implicit val emptyOrDetailsUnmarshaller = Unmarshaller.eitherUnmarshaller[EmptyResponse, JobDetails]
}
