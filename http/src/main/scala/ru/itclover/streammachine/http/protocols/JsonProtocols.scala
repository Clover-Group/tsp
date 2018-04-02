package ru.itclover.streammachine.http.protocols

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import ru.itclover.streammachine.http.domain.input.FindPatternsRequest
import ru.itclover.streammachine.http.domain.output.{FailureResponse, SuccessfulResponse}
import ru.itclover.streammachine.io.input.{InputConf, JDBCInputConf, JDBCNarrowInputConf, RawPattern}
import ru.itclover.streammachine.io.output.{JDBCOutputConf, OutputConf, PGSegmentsSink}
import spray.json.{DefaultJsonProtocol, JsonFormat}


trait JsonProtocols extends SprayJsonSupport with DefaultJsonProtocol {
  implicit def sResponseFmt[R: JsonFormat] = jsonFormat1(SuccessfulResponse.apply[R])
  implicit val fResponseFmt = jsonFormat3(FailureResponse.apply)

  implicit val jdbcInpConfFmt = jsonFormat9(JDBCInputConf.apply)
  implicit val jdbcNarrowInpConfFmt = jsonFormat4(JDBCNarrowInputConf.apply)
  implicit val jdbcSinkSchemaFmt = jsonFormat(PGSegmentsSink.apply, "tableName", "sourceIdFieldVal", "beginField",
    "endField", "appIdField", "patternIdField", "processingTimeField", "contextField", "forwardedFields")
  implicit val jdbcOutConfFmt = jsonFormat6(JDBCOutputConf.apply)

  implicit val rawPatternFmt = jsonFormat3(RawPattern.apply)
  implicit def patternsRequestFmt[IN <: InputConf : JsonFormat, OUT <: OutputConf : JsonFormat] =
    jsonFormat3(FindPatternsRequest.apply[IN, OUT])
}
