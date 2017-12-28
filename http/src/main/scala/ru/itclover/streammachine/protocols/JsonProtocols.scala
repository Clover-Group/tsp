package ru.itclover.streammachine.protocols

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import ru.itclover.streammachine.domain.output.{FailureResponse, SuccessfulResponse}
import ru.itclover.streammachine.io.input.JDBCConfig
import spray.json.DefaultJsonProtocol

trait JsonProtocols extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val sResponseFmt = jsonFormat2(SuccessfulResponse.apply)
  implicit val fResponseFmt = jsonFormat3(FailureResponse.apply)

  implicit val jdbcConfFmt = jsonFormat5(JDBCConfig.apply)
}
