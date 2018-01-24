package ru.itclover.streammachine.http.protocols

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import ru.itclover.streammachine.http.domain.input.FindPatternsRequest
import ru.itclover.streammachine.http.domain.output.{FailureResponse, SuccessfulResponse}
import ru.itclover.streammachine.io.input.{JDBCInputConfig => InputConfigs}
import ru.itclover.streammachine.io.output.{JDBCSegmentsSink, JDBCOutputConfig => OutputConfigs}
import spray.json.{DefaultJsonProtocol, JsonFormat}

trait JsonProtocols extends SprayJsonSupport with DefaultJsonProtocol {
  implicit def sResponseFmt[R: JsonFormat] = jsonFormat1(SuccessfulResponse.apply[R])
  implicit val fResponseFmt = jsonFormat3(FailureResponse.apply)

  implicit val jdbcInpConfFmt = jsonFormat7 (InputConfigs.apply)
  implicit val jdbcSinkSchemaFmt = jsonFormat(JDBCSegmentsSink.apply, "tableName", "fromTimeField", "fromTimeMillisField",
    "toTimeField", "toTimeMillisField", "patternIdField", "forwardedFields")
  implicit val jdbcOutConfFmt = jsonFormat6(OutputConfigs.apply)

  implicit val patternsRequestFmt = jsonFormat3(FindPatternsRequest.apply)
}
