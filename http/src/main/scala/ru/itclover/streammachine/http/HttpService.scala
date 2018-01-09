package ru.itclover.streammachine.http

import akka.actor.ActorSystem
import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.server.{Directives, ExceptionHandler, RequestContext, Route}
import akka.stream.ActorMaterializer
import ru.itclover.streammachine.http.domain.output.{FailureResponse, SuccessfulResponse}
import ru.itclover.streammachine.http.protocols.JsonProtocols
import ru.itclover.streammachine.http.routes.FindPatternRangesRoute
import ru.itclover.streammachine.io.input.{ClickhouseInput, JDBCConfig => InputConfigs}
import ru.itclover.streammachine.io.output.{JDBCConfig => OutputConfigs}

import scala.concurrent.ExecutionContextExecutor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.types.Row
import ru.itclover.streammachine.{FlinkStateMachineMapper, SegmentResultsMapper}
import ru.itclover.streammachine.core.PhaseParser
import ru.itclover.streammachine.core.Time.TimeExtractor
import ru.itclover.streammachine.http.domain.input.IORequest
import ru.itclover.streammachine.http.utils.EvalUtils
import com.typesafe.scalalogging.Logger

trait HttpService extends Directives with JsonProtocols with FindPatternRangesRoute {
  val isDebug = false

  val log = Logger[HttpService]

  val defaultErrorsHandler: ExceptionHandler = ExceptionHandler {
    case ex: Exception =>
      val reason = if (isDebug) ex.getMessage else ""
      complete(FailureResponse("Internal server error", 500, reason))
  }

  override val route: Route =
    handleExceptions(defaultErrorsHandler) {
      path("streaming" / "find-patterns") {
//        requestEntityPresent {
          //          entity(as[IORequest]) { ioRequest =>
          parameters('phaseCode.as[String].?) {
            case (Some(phaseCode)) => {
              //                val (input, output) = (ioRequest.source, ioRequest.sink)
              val input = InputConfigs(
                jdbcUrl = "jdbc:clickhouse://localhost:8123/renamedTest",
                query = "select date, timestamp, Wagon_id, SpeedEngine, ContuctorOilPump from series765_data_test_speed limit 0, 30000",
                driverName = "ru.yandex.clickhouse.ClickHouseDriver"
              )
              val output = OutputConfigs(
                jdbcUrl = "jdbc:clickhouse://localhost:8123/renamedTest",
                sinkTable = "series765_data_sink_test_speed",
                sinkColumnsNames = List[Symbol]('if_rule_success),
                driverName = "ru.yandex.clickhouse.ClickHouseDriver",
                batchInterval = Some(1000)
              )

              val streamEnv = StreamExecutionEnvironment.createLocalEnvironment()

              val fieldsTypesInfo = ClickhouseInput.queryFieldsTypeInformation(input) match {
                case Right(typesInfo) => typesInfo
                case Left(err) => throw err
              }
              val fieldsIndexesMap = fieldsTypesInfo.map(_._1).map(Symbol(_)).zipWithIndex.toMap

              val chInputFormat = ClickhouseInput.getInputFormat(input, fieldsTypesInfo.toArray)

              val phase: PhaseParser[Row, Any, Any] = EvalUtils.evalPhaseUsingRowExtractors(phaseCode, 0, fieldsIndexesMap)

              val timeExtractor: TimeExtractor[Row] = new TimeExtractor[Row] {
                override def apply(v1: Row) = {
                  v1.getField(1).asInstanceOf[java.sql.Timestamp]
                }
              }

              val stateMachine = FlinkStateMachineMapper(phase, segmentMapper(phase, timeExtractor))

              val dataStream = streamEnv.createInput(chInputFormat)
              val resultStream = dataStream.keyBy(row => row.getField(2)).flatMap(stateMachine)
//              val resultStream = dataStream.map(r => r.getField(3))
              resultStream.map(result => println(s"R = $result"))

              log.info(s"Start pattern finding with phaseCode = `$phaseCode`")
              val result = streamEnv.execute()


              complete(SuccessfulResponse(1))
            }
            case _ =>
              complete(FailureResponse("phaseCode GET arg is required"))
          }
        }
        //        }
//      }
    }

  def segmentMapper[Event, PhaseOut](p: PhaseParser[Event, _, PhaseOut], te: TimeExtractor[Event]) =
    SegmentResultsMapper[Event, PhaseOut]()(te)
}
