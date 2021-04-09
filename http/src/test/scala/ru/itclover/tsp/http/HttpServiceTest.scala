package ru.itclover.tsp.http
import java.util.concurrent.{SynchronousQueue, ThreadPoolExecutor, TimeUnit}
import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.spark.sql.SparkSession
import org.scalatest.Inspectors._
import org.scalatest.{FlatSpec, Matchers}
import ru.itclover.tsp.http.domain.output.FailureResponse
import ru.itclover.tsp.http.protocols.RoutesProtocols
import ru.itclover.tsp.http.utils.Exceptions.InvalidRequest
import ru.yandex.clickhouse.except.ClickHouseException

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor}

// In test cases, 'should' expressions are non-unit. Suppressing wartremover warnings about it
@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class HttpServiceTest extends FlatSpec with Matchers with ScalatestRouteTest with RoutesProtocols {

  case class TestHttpService(override val isHideExceptions: Boolean) extends HttpService {
    implicit val system: ActorSystem = ActorSystem("TSP-system-test")
    implicit val materializer: ActorMaterializer = ActorMaterializer()
    implicit val executionContext: ExecutionContextExecutor = system.dispatcher

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("TSP Spark test")
      .config("spark.io.compression.codec", "snappy")
      .getOrCreate()

    // to run blocking tasks.
    val blockingExecutorContext: ExecutionContextExecutor =
      ExecutionContext.fromExecutor(
        new ThreadPoolExecutor(
          0, // corePoolSize
          Int.MaxValue, // maxPoolSize
          1000L, //keepAliveTime
          TimeUnit.MILLISECONDS, //timeUnit
          new SynchronousQueue[Runnable](), //workQueue
          new ThreadFactoryBuilder().setNameFormat("blocking-thread").setDaemon(true).build()
        )
      )
  }

  val services = Seq(TestHttpService(false), TestHttpService(true))

  "HTTP service" should "properly handle exceptions" in {
    forAll(services) { service =>
      Get() ~> service.exceptionsHandler(new Exception()) ~> check {
        response.status shouldBe StatusCodes.InternalServerError
        noException should be thrownBy Unmarshal(response.entity).to[FailureResponse]
        val resp = Await.result(Unmarshal(response.entity).to[FailureResponse], Duration.Inf)
        resp.errorCode shouldBe 5008
        resp.message shouldBe "Request handling failure"
        resp.errors.isEmpty shouldBe service.isHideExceptions
      }
      Get() ~> service.exceptionsHandler(new Exception("test", new Exception())) ~> check {
        response.status shouldBe StatusCodes.InternalServerError
        noException should be thrownBy Unmarshal(response.entity).to[FailureResponse]
        val resp = Await.result(Unmarshal(response.entity).to[FailureResponse], Duration.Inf)
        resp.errorCode shouldBe 5008
        resp.message shouldBe "Request handling failure"
        resp.errors.isEmpty shouldBe service.isHideExceptions
      }
      Get() ~> service.exceptionsHandler(new ClickHouseException(54, new Exception(), "127.0.0.1", 8123)) ~> check {
        response.status shouldBe StatusCodes.InternalServerError
        noException should be thrownBy Unmarshal(response.entity).to[FailureResponse]
        val resp = Await.result(Unmarshal(response.entity).to[FailureResponse], Duration.Inf)
        resp.errorCode shouldBe 5001
        resp.message shouldBe "Job execution failure"
        resp.errors.isEmpty shouldBe service.isHideExceptions
      }
      Get() ~> service.exceptionsHandler(new ClickHouseException(54, null, "127.0.0.1", 8123)) ~> check {
        response.status shouldBe StatusCodes.InternalServerError
        noException should be thrownBy Unmarshal(response.entity).to[FailureResponse]
        val resp = Await.result(Unmarshal(response.entity).to[FailureResponse], Duration.Inf)
        resp.errorCode shouldBe 5001
        resp.message shouldBe "Job execution failure"
        resp.errors.isEmpty shouldBe service.isHideExceptions
      }
      Get() ~> service.exceptionsHandler(InvalidRequest("test")) ~> check {
        response.status shouldBe StatusCodes.BadRequest
        noException should be thrownBy Unmarshal(response.entity).to[FailureResponse]
        val resp = Await.result(Unmarshal(response.entity).to[FailureResponse], Duration.Inf)
        resp.errorCode shouldBe 4005
        resp.message shouldBe "Invalid request"
        resp.errors.isEmpty shouldBe false
      }
      Get() ~> service.exceptionsHandler(new RuntimeException()) ~> check {
        response.status shouldBe StatusCodes.InternalServerError
        noException should be thrownBy Unmarshal(response.entity).to[FailureResponse]
        val resp = Await.result(Unmarshal(response.entity).to[FailureResponse], Duration.Inf)
        resp.errorCode shouldBe 5005
        resp.message shouldBe "Request handling failure"
        resp.errors.isEmpty shouldBe service.isHideExceptions
      }
      Get() ~> service.exceptionsHandler(new RuntimeException("test", new Exception())) ~> check {
        response.status shouldBe StatusCodes.InternalServerError
        noException should be thrownBy Unmarshal(response.entity).to[FailureResponse]
        val resp = Await.result(Unmarshal(response.entity).to[FailureResponse], Duration.Inf)
        resp.errorCode shouldBe 5005
        resp.message shouldBe "Request handling failure"
        resp.errors.isEmpty shouldBe service.isHideExceptions
      }
    }
  }

  "FailureResponse objects" should "construct" in {
    FailureResponse(new Exception()).errorCode shouldBe 5000
    FailureResponse(5011, new Exception()).errorCode shouldBe 5011
  }
}
