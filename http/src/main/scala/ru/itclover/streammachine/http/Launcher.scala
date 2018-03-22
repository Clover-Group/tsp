package ru.itclover.streammachine.http

import akka.actor.{Actor, ActorSystem, PoisonPill, Props, Terminated}
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.io.StdIn


object Launcher extends App with HttpService {
  override val isDebug: Boolean = ConfigFactory.load().getBoolean("general.is-debug")
  private val isListenStdIn = ConfigFactory.load().getBoolean("general.is-follow-input")
  private val log = Logger("Launcher")

  implicit val system: ActorSystem = ActorSystem("my-system")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher
  val streamEnvironment = StreamExecutionEnvironment.createLocalEnvironment()

  private val host = "0.0.0.0"
  private val port = 8080
  val bindingFuture = Http().bindAndHandle(route, host, port)

  log.info(s"Service online at http://$host:$port/" + (if (isDebug) " in debug mode." else ""))

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
