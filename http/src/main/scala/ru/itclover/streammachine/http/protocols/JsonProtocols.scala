package ru.itclover.streammachine.http.protocols

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import ru.itclover.streammachine.http.domain.input.FindPatternsRequest
import ru.itclover.streammachine.http.domain.output.{FailureResponse, SuccessfulResponse}
import ru.itclover.streammachine.io.input.{InfluxDBInputConf, InputConf, JDBCInputConf, RawPattern}
import ru.itclover.streammachine.io.output.{JDBCOutputConf, OutputConf, RowSchema}
import spray.json.{DefaultJsonProtocol, JsonFormat}


trait JsonProtocols extends SprayJsonSupport with DefaultJsonProtocol {
  implicit def sResponseFmt[R: JsonFormat] = jsonFormat1(SuccessfulResponse.apply[R])
  implicit val fResponseFmt = jsonFormat3(FailureResponse.apply)

  implicit val jdbcInpConfFmt = jsonFormat(JDBCInputConf.apply, "sourceId", "jdbcUrl", "query", "driverName",
    "datetimeField", "eventsMaxGapMs", "partitionFields", "userName", "password")
  implicit val influxInpConfFmt = jsonFormat(InfluxDBInputConf.apply, "sourceId", "dbName", "url",
    "query", "eventsMaxGapMs", "partitionFields", "datetimeFields", "userName", "password")

  implicit val rowSchemaFmt = jsonFormat(RowSchema.apply, "sourceIdField", "fromTsField", "toTsField",
    "appIdFieldVal", "patternIdField", "processingTsField", "contextField", "forwardedFields")
  // implicit val jdbcSinkSchemaFmt = jsonFormat(JDBCSegmentsSink.apply, "tableName", "rowSchema")
  implicit val jdbcOutConfFmt = jsonFormat7(JDBCOutputConf.apply)

  implicit val rawPatternFmt = jsonFormat4(RawPattern.apply)
  implicit def patternsRequestFmt[IN <: InputConf[_] : JsonFormat, OUT <: OutputConf : JsonFormat] =
    jsonFormat3(FindPatternsRequest.apply[IN, OUT])
}
