package ru.itclover.streammachine.http.protocols

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import ru.itclover.streammachine.http.domain.input.{DSLPatternRequest, FindPatternsRequest}
import ru.itclover.streammachine.http.domain.output.{ExecTime, FailureResponse, FinishedJobResponse, SuccessfulResponse}
import ru.itclover.streammachine.io.input.{InfluxDBInputConf, InputConf, JDBCInputConf, RawPattern}
import ru.itclover.streammachine.io.output.{JDBCOutputConf, OutputConf, RowSchema}
import spray.json.{DefaultJsonProtocol, JsonFormat}


trait RoutesProtocols extends SprayJsonSupport with DefaultJsonProtocol {
  implicit def sResponseFmt[R: JsonFormat] = jsonFormat2(SuccessfulResponse.apply[R])
  implicit val execTimeFmt = jsonFormat1(ExecTime.apply)
  implicit val finishedJobResponseFmt = jsonFormat2(FinishedJobResponse.apply)

  implicit val fResponseFmt = jsonFormat3(FailureResponse.apply)

  implicit val jdbcInpConfFmt = jsonFormat(JDBCInputConf.apply, "sourceId", "jdbcUrl", "query", "driverName",
    "datetimeField", "eventsMaxGapMs", "partitionFields", "userName", "password", "parallelism")
  implicit val influxInpConfFmt = jsonFormat(InfluxDBInputConf.apply, "sourceId", "dbName", "url",
    "query", "eventsMaxGapMs", "partitionFields", "datetimeField", "userName", "password", "parallelism", "timeoutSec")

  implicit val rowSchemaFmt = jsonFormat(RowSchema.apply, "sourceIdField", "fromTsField", "toTsField",
    "appIdFieldVal", "patternIdField", "processingTsField", "contextField", "forwardedFields")
  // implicit val jdbcSinkSchemaFmt = jsonFormat(JDBCSegmentsSink.apply, "tableName", "rowSchema")
  implicit val jdbcOutConfFmt = jsonFormat7(JDBCOutputConf.apply)

  implicit val rawPatternFmt = jsonFormat4(RawPattern.apply)
  implicit def patternsRequestFmt[IN <: InputConf[_] : JsonFormat, OUT <: OutputConf[_] : JsonFormat] =
    jsonFormat4(FindPatternsRequest.apply[IN, OUT])
  implicit val dslPatternFmt = jsonFormat1(DSLPatternRequest.apply)
}
