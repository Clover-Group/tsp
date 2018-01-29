package ru.itclover.streammachine.http

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import com.typesafe.scalalogging.Logger
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.concurrent.duration._
import scala.io.StdIn

object DockerWebServer extends App with HttpService {
  override val isDebug: Boolean = true

  implicit val system: ActorSystem = ActorSystem("my-system")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  val streamEnvironment = StreamExecutionEnvironment.createLocalEnvironment()

  val bindingFuture = Http().bindAndHandle(route, "0.0.0.0", 8080)

  println(s"Server online at http://localhost:8080/\nPress RETURN to stop...")

  // shutdown Hook
  scala.sys.addShutdownHook {
    log.info("Terminating...")
    system.terminate()
    Await.result(system.whenTerminated, 30.seconds)
    log.info("Terminated... Bye!")
  }
}
