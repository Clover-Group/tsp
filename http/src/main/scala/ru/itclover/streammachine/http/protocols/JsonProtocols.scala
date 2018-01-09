package ru.itclover.streammachine.http.protocols

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import ru.itclover.streammachine.http.domain.input.IORequest
import ru.itclover.streammachine.http.domain.output.{FailureResponse, SuccessfulResponse}
import ru.itclover.streammachine.io.input.{JDBCConfig => InputConfigs}
import ru.itclover.streammachine.io.output.{JDBCConfig => OutputConfigs}
import spray.json.DefaultJsonProtocol

trait JsonProtocols extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val sResponseFmt = jsonFormat2(SuccessfulResponse.apply)
  implicit val fResponseFmt = jsonFormat3(FailureResponse.apply)

  implicit val jdbcInpConfFmt = jsonFormat5(InputConfigs.apply)
  implicit val jdbcOutConfFmt = jsonFormat7(OutputConfigs.apply)

  implicit val ioConfFmt = jsonFormat2(IORequest.apply)
}
