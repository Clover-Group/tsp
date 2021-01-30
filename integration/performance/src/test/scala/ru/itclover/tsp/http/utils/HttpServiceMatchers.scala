package ru.itclover.tsp.http.utils

import java.util.concurrent.{SynchronousQueue, ThreadPoolExecutor, TimeUnit}

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.typesafe.scalalogging.Logger
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.scalatest.{Matchers, Suite}
import ru.itclover.tsp.http.HttpService
import ru.itclover.tsp.http.domain.output.SuccessfulResponse.FinishedJobResponse

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.util.Success

// In test cases, 'should' expressions are non-unit. Suppressing wartremover warnings about it
// We suppress Any for `shouldBe empty` statements.
@SuppressWarnings(Array(
  "org.wartremover.warts.NonUnitStatements",
  "org.wartremover.warts.Any"
))
trait HttpServiceMatchers extends ScalatestRouteTest with Matchers with HttpService { self: Suite =>

  implicit override val executionContext: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global
  implicit override val streamEnvironment = StreamExecutionEnvironment.createLocalEnvironment()
  streamEnvironment.setMaxParallelism(30000) // For proper keyBy partitioning

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

  implicit def defaultTimeout = RouteTestTimeout(300.seconds)

  val log = Logger("HttpServiceMathers")

  /** Util for checking segments count and size in seconds */
  // Here, default argument for `epsilon` is useful.
  @SuppressWarnings(Array("org.wartremover.warts.DefaultArguments"))
  def checkByQuery(expectedValues: Seq[Double], query: String, epsilon: Double = 0.0001)
                  (implicit container: JDBCContainer): Unit = {
    val resultSet = container.executeQuery(query)
    for (expectedVal <- expectedValues) {
      resultSet.next() shouldEqual true
      val value = resultSet.getDouble(1)
      value should === (expectedVal +- epsilon)
    }
  }

  def checkAndGetExecTimeSec(): Double = {
    status shouldEqual StatusCodes.OK
    val resp = unmarshal[FinishedJobResponse](responseEntity)
    resp shouldBe a[Success[_]]
    val execTimeS = resp.map(_.response.execTimeSec).getOrElse(Double.NaN)
    if (execTimeS.isNaN)
      log.warn(s"Test job probably failed.")
    else
      log.info(s"Test job completed for $execTimeS sec.")
    execTimeS
  }
}
