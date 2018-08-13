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
import cats._
import cats.implicits._
import org.apache.flink.client.program.PackagedProgram
import org.apache.flink.configuration.{ConfigConstants, Configuration}


object Launcher extends App with HttpService {
  private val configs = ConfigFactory.load()
  override val isDebug: Boolean = configs.getBoolean("general.is-debug")
  private val isListenStdIn = configs.getBoolean("general.is-follow-input")
  private val log = Logger("Launcher")

  implicit val system: ActorSystem = ActorSystem("StreamMachine-system")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher


  val conf: Configuration = new Configuration()
  conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)
  conf.setInteger("rest.port", configs.getInt("http.flink-monitoring.port"))
  val streamEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
  streamEnvironment.setMaxParallelism(configs.getInt("flink.max-parallelism"))

  val a = new PackagedProgram()


  private val host = configs.getString("http.host")
  private val port = configs.getInt("http.port")
  val bindingFuture = Http().bindAndHandle(route, host, port)

  log.info(s"Service online at http://$host:$port/" + (if (isDebug) " in debug mode." else ""))

  if (isListenStdIn) {
    log.info("Press RETURN to stop...")
    StdIn.readLine()
    log.info("Terminating...")
    bindingFuture
      .flatMap(_.unbind())
      .onComplete(_ => Await.result(
        system.whenTerminated.map(_ => log.info("Terminated... Bye!")),
        60.seconds)
      )
  } else {
    scala.sys.addShutdownHook {
      log.info("Terminating...")
      system.terminate()
      Await.result(system.whenTerminated, 60.seconds)
      log.info("Terminated... Bye!")
    }
  }
}
