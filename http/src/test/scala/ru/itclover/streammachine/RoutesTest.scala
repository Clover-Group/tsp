package ru.itclover.streammachine

import org.scalatest.{Matchers, WordSpec}
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.http.scaladsl.server._
import Directives._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import ru.itclover.streammachine.http.HttpService

import scala.concurrent.ExecutionContextExecutor

class RoutesTest extends WordSpec with Matchers with ScalatestRouteTest with HttpService{
  override implicit val executionContext: ExecutionContextExecutor = scala.concurrent.ExecutionContext.Implicits.global
  val streamEnvironment: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment()

  "streaming/find-patterns" should {
    "find basic patterns" in {
      Get("streaming/find-patterns") ~> route ~> check {
        responseAs[String] shouldEqual "PONG!"
      }
    }
  }

}
