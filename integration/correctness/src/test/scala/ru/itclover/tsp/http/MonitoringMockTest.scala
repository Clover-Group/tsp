package ru.itclover.tsp.http
import akka.actor.ActorSystem
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.server.Directives
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.stream.ActorMaterializer
import org.apache.spark.sql.SparkSession
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{AsyncFlatSpec, BeforeAndAfter, Matchers}
import ru.itclover.tsp.http.routes.MonitoringRoutes
import ru.itclover.tsp.http.services.streaming.FlinkMonitoringService
import ru.itclover.tsp.http.services.streaming.MonitoringServiceModel.MetricInfo
import ru.itclover.tsp.http.utils.MockServer

import scala.concurrent._

class MonitoringMockTest
    extends AsyncFlatSpec
    with ScalatestRouteTest
    with Matchers
    with Directives
    with BeforeAndAfter
    with ScalaFutures {

  val port = 9034
  private var t: Thread = _

  before {
    t = new Thread(() => MockServer.startServer("127.0.0.1", port))
    t.start()
    Thread.sleep(1000)
  }

  after {
    t.join(1000)
  }

  "Monitoring service" should "work with mocked Flink service" in {
    val monitoringService = FlinkMonitoringService(s"http://127.0.0.1:$port")
    monitoringService.queryJobsOverview.map { res =>
      assert(res.jobs.length == 2)
    }

    monitoringService.queryJobByName("job1").map { res =>
      assert(res.isDefined)
    }
    monitoringService.queryJobByName("job2").map { res =>
      assert(res.isDefined)
    }
    monitoringService.queryJobByName("job3").map { res =>
      assert(res.isEmpty)
    }

    monitoringService.queryJobExceptions("one").map { res =>
      assert(res.isEmpty)
    }
    monitoringService.queryJobInfo("job1").map { res =>
      assert(res.map(x => x.jid).getOrElse("error") == "1")
    }

    monitoringService.sendStopQuery("job1").map { res =>
      assert(res.isDefined)
    }
    monitoringService.sendStopQuery("job2").map { res =>
      assert(res.isDefined)
    }
    monitoringService.sendStopQuery("job3").map { res =>
      assert(res.isEmpty)
    }
  }

  "Monitoring routes" should "work" in {
    val monitoringRoutes = new MonitoringRoutes {
      implicit override val actors: ActorSystem = ActorSystem("TSP-monitoring-test")
      implicit override val materializer: ActorMaterializer = ActorMaterializer()(actors)
      implicit override val executionContext: ExecutionContextExecutor = system.dispatcher
      override val uri: Uri = s"http://127.0.0.1:$port"
      override val spark = SparkSession.builder()
        .master("local")
        .appName("TSP Spark test")
        .config("spark.io.compression.codec", "snappy")
        .getOrCreate()
    }
    Get("/metainfo/getVersion") ~> monitoringRoutes.route ~> check {
      response.status shouldBe StatusCodes.OK
    }

    Get("/jobs/overview") ~> monitoringRoutes.route ~> check {
      response.status shouldBe StatusCodes.OK
    }

    Get("/job/3/status") ~> monitoringRoutes.route ~> check {
      response.status shouldBe StatusCodes.BadRequest
    }

    Get("/job/3/exceptions") ~> monitoringRoutes.route ~> check {
      response.status shouldBe StatusCodes.BadRequest
    }

    Get("/job/3/stop") ~> monitoringRoutes.route ~> check {
      response.status shouldBe StatusCodes.BadRequest
    }
  }
}
