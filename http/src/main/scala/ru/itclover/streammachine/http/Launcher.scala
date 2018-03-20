package ru.itclover.streammachine.http

import akka.actor.{Actor, ActorSystem, PoisonPill, Props, Terminated}
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import com.typesafe.scalalogging.Logger
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.io.StdIn


object Launcher extends App with HttpService {
  private val isListenStdIn = args.headOption.map(_.toBoolean).getOrElse(false)
  private val log = Logger("Launcher")

  override val isDebug: Boolean = args.headOption.map(_.toBoolean).getOrElse(false)
  implicit val system: ActorSystem = ActorSystem("my-system")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher
  val streamEnvironment = StreamExecutionEnvironment.createLocalEnvironment()

  private val host = "0.0.0.0"
  private val port = 8080
  val bindingFuture = Http().bindAndHandle(route, host, port)

  log.info(s"Service online at http://$host:$port/")

  if (isListenStdIn) {
    log.info("Press RETURN to stop...")
    StdIn.readLine()
    bindingFuture
      .flatMap(_.unbind())
      .onComplete(_ => system.terminate())
  } else {
    scala.sys.addShutdownHook {
      log.info("Terminating...")
      system.terminate()
      Await.result(system.whenTerminated, 60.seconds)
      log.info("Terminated... Bye!")
    }
  }
}
