package ru.itclover.tsp.http.utils
import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.server.{HttpApp, Route}
import ru.itclover.tsp.http.services.flink.MonitoringServiceModel._
import ru.itclover.tsp.http.services.flink.MonitoringServiceProtocols

import scala.concurrent.{ExecutionContext, Future, Promise, blocking}

object MockServer extends HttpApp with MonitoringServiceProtocols {
  private var shouldStop = false
  private var runs = false

  def isRunning: Boolean = runs

  override def routes: Route =
    path("jobs" / "overview"./) {
      get {
        complete(
          JobsOverview(List(JobBrief("1", "job1"), JobBrief("2", "job2")))
        )
      }
    } ~ path("jobs" / "1"./) {
      get {
        complete(
          JobDetails("1", "job1", "FINISHED", 0, 1, Vector(Vertex("101", "v101", VertexMetrics(100, 100, Some(0)))))
        )
      }
    } ~ path("jobs" / "2"./) {
      get {
        complete(
          JobDetails("2", "job2", "FINISHED", 0, 1, Vector(Vertex("201", "v201", VertexMetrics(100, 100, Some(0)))))
        )
      }
    } ~ path("jobs" / "1"./) {
      patch {
        complete(EmptyResponse())
      }
    } ~ path("jobs" / "2"./) {
      patch {
        complete(EmptyResponse())
      }
    } ~ path("jobs" / "1" / "vertices" / "101" / "metrics") {
      parameter('get ? "") { metricList =>
        get {
          val metrics = metricList.split(",")
          if (metrics.isEmpty) {
            complete(
              List(MetricName("metric1.1"), MetricName("metric1.2"))
            )
          } else {
            complete(
              List(Metric("metric1.1", "1001"), Metric("metric1.2", "1002")).filter(m => metrics.contains(m.id))
            )
          }
        }
      }
    } ~ path("jobs" / "2" / "vertices" / "201" / "metrics") {
      parameter('get ? "") { metricList =>
        get {
          val metrics = metricList.split(",")
          if (metrics.isEmpty) {
            complete(
              List(MetricName("metric2.1"))
            )
          } else {
            complete(
              List(Metric("metric2.1", "2001")).filter(m => metrics.contains(m.id))
            )
          }
        }
      }
    }

//  implicit def rejectionsHandler: RejectionHandler = RejectionHandler
//    .newBuilder()
//    .handleNotFound {
//      extractUnmatchedPath { p =>
//        complete(StatusCodes.NotFound, List(`Content-Type`(`application/json`)), "Path not found: `$p`")
//      }
//    }
//    .result()

  override def waitForShutdownSignal(
    actorSystem: ActorSystem
  )(implicit executionContext: ExecutionContext): Future[Done] = {
    val promise = Promise[Done]()
    sys.addShutdownHook {
      runs = false
      promise.trySuccess(Done)
    }
    Future {
      blocking {
        if (shouldStop) {
          runs = false
          promise.trySuccess(Done)
        }
      }
    }
    runs = false
    promise.future
  }

  override def startServer(host: String, port: Int): Unit = {
    runs = true
    super.startServer(host, port)
  }

  def stop(): Unit = { shouldStop = true }
}
