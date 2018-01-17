package ru.itclover.streammachine.http

import akka.actor.ActorSystem
import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.server.{Directives, ExceptionHandler, RequestContext, Route}
import akka.stream.ActorMaterializer
import ru.itclover.streammachine.http.domain.output.{FailureResponse, SuccessfulResponse}
import ru.itclover.streammachine.http.protocols.JsonProtocols
import ru.itclover.streammachine.http.routes.FindPatternRangesRoute
import ru.itclover.streammachine.io.input
import ru.itclover.streammachine.io.input.{ClickhouseInput, StorageFormat}
import ru.itclover.streammachine.io.output

import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import ru.itclover.streammachine.{FlinkStream, SegmentResultsMapper}
import com.typesafe.scalalogging.Logger
import ru.itclover.streammachine.http.domain.input.IORequest


trait HttpService extends Directives with JsonProtocols with FindPatternRangesRoute {
  val isDebug = false

  val log = Logger[HttpService]

  val defaultErrorsHandler: ExceptionHandler = ExceptionHandler {
    case ex: Exception =>
      ex.printStackTrace()
      val reason = if (isDebug) ex.getMessage else ""
      complete(FailureResponse(-1, "Internal server error", reason :: Nil))
  }

  def streamEnvironment: StreamExecutionEnvironment

  override val route: Route =
    handleExceptions(defaultErrorsHandler) {
      path("streaming" / "find-patterns" / "wide-dense-table" /) {
        requestEntityPresent {
          entity(as[IORequest]) { ioRequest =>
            parameters('phaseCode.as[String]) { phaseCode =>

              val (inConf, outConf) = (ioRequest.source, ioRequest.sink)

              val (streamEnv, stream) = FlinkStream.createPatternSearchStream(inConf, outConf, phaseCode,
                StorageFormat.WideAndDense)(streamEnvironment)

              stream.map(result => println(s"R = $result"))

              val resultFuture = Future {
                log.info(s"Start pattern finding with phaseCode = `$phaseCode`")
                val t0 = System.nanoTime()
                val result = streamEnv.execute()
                log.info(s"Finish pattern finding for ${(System.nanoTime() - t0) / 1000000000.0} seconds.")
                result
              }

              onSuccess(resultFuture) {
                jobResult => complete(SuccessfulResponse(jobResult.hashCode))
              }
            }
          }
        }
      }
    }

}
