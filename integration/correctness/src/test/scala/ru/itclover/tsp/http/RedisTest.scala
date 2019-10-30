package ru.itclover.tsp.http

import java.util.concurrent.{SynchronousQueue, ThreadPoolExecutor, TimeUnit}

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import com.dimafeng.testcontainers.{Container, ForAllTestContainer, GenericContainer}
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.scalatest.{FlatSpec, Matchers}
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper
import ru.itclover.tsp.core.RawPattern
import ru.itclover.tsp.http.domain.input.FindPatternsRequest
import ru.itclover.tsp.io.input.RedisInputConf

import scala.util.Random
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import ru.itclover.tsp.io.input.SerializerInfo
import ru.itclover.tsp.io.output.{RedisOutputConf, RowSchema}
import ru.itclover.tsp.services.RedisService

class RedisTest extends FlatSpec with ScalatestRouteTest with HttpService with ForAllTestContainer with Matchers{

  implicit def defaultTimeout = RouteTestTimeout(300.seconds)

  override implicit val streamEnvironment: StreamExecutionEnvironment =
    StreamExecutionEnvironment.createLocalEnvironment()

  override implicit val executionContext: ExecutionContextExecutor = ExecutionContext.global

  override val blockingExecutorContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(
    new ThreadPoolExecutor(
      0, // corePoolSize
      Int.MaxValue, // maxPoolSize
      1000L, //keepAliveTime
      TimeUnit.MILLISECONDS, //timeUnit
      new SynchronousQueue[Runnable](), //workQueue
      new ThreadFactoryBuilder().setNameFormat("blocking-thread").setDaemon(true).build()
    )
  )

  override val container: GenericContainer = new GenericContainer(
    "redis:latest",
    exposedPorts = Seq(6380)
  )

  val inputConf = RedisInputConf(
    host=container.containerIpAddress,
    port=container.exposedPorts.head,
    datetimeField='dt,
    partitionFields=Seq('stocknum),
    fieldsTypes = Map(
      "test_int" -> "int8",
      "test_string" -> "string"
    ),
    serializationInfo=Seq(
      SerializerInfo(
        key="test_key",
        serializerType="json"
      )
    )
  )

  val sinkSchema = RowSchema('series_storage, 'from, 'to, ('app, 1), 'id, 'timestamp, 'context, inputConf.partitionFields)

  val outputConf = RedisOutputConf(
    host=container.containerIpAddress,
    port=container.exposedPorts.head,
    outputInfo = SerializerInfo(
      key="test_key",
      serializerType="json"
    ),
    rowSchema = sinkSchema
  )

  val (timeRangeSec, assertions) = (1 to 80) -> Seq(
    RawPattern("6", "test_int > 20")
  )

  override def afterStart(): Unit = {
    super.beforeAll()

    val serializationInfo = inputConf.serializationInfo.head

    val (client, _) = RedisService.clientInstance(inputConf, inputConf.serializationInfo.head)
    val rnd = new Random()

    val testData = Map(
      "dt" -> rnd.nextFloat(),
      "stock_num" -> "0017",
      "test_int" -> rnd.nextInt(100),
      "test_string" -> "test"
    )

    val mapper = new ObjectMapper()
    val jsonString = mapper.writeValueAsString(testData)

    client.set[Array[Byte]](serializationInfo.key, jsonString.getBytes("UTF-8")).value.get.get

  }

  "Redis test assertions" should "work for redis source" in {

    Post("/streamJob/from-redis/to-redis/?run_async=0", FindPatternsRequest("1", inputConf, outputConf, assertions)) ~>
    route ~> check {
      status shouldBe StatusCodes.OK
    }

  }

}
