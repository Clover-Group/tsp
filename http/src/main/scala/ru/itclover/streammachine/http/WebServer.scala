package ru.itclover.streammachine.http

import akka.actor.{Actor, ActorSystem, PoisonPill, Props, Terminated}
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import scala.concurrent.ExecutionContextExecutor
import scala.io.StdIn

object WebServer extends App with HttpService {
  override val isDebug: Boolean = true

  implicit val system: ActorSystem = ActorSystem("my-system")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  val streamEnvironment = StreamExecutionEnvironment.createLocalEnvironment()

  val bindingFuture = Http().bindAndHandle(route, "localhost", 8080)

  println(s"Server online at http://localhost:8080/\nPress RETURN to stop...")
  StdIn.readLine()
  bindingFuture
    .flatMap(_.unbind())
    .onComplete(_ => system.terminate())
}
