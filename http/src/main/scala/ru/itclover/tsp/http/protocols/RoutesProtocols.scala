package ru.itclover.tsp.http.protocols

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import ru.itclover.tsp.http.domain.input.{DSLPatternRequest, FindPatternsRequest}
import ru.itclover.tsp.http.domain.output.{ExecInfo, FailureResponse, FinishedJobResponse, SuccessfulResponse}
import ru.itclover.tsp.io.input.{InfluxDBInputConf, InputConf, JDBCInputConf, RawPattern}
import ru.itclover.tsp.io.output.{JDBCOutputConf, OutputConf, RowSchema}
import spray.json.{DefaultJsonProtocol, JsArray, JsBoolean, JsNumber, JsString, JsValue, JsonFormat}

trait RoutesProtocols extends SprayJsonSupport with DefaultJsonProtocol {
  implicit object propertyFormat extends JsonFormat[AnyRef] {
    override def write(obj: AnyRef): JsValue = obj match {
      case i: java.lang.Integer => JsNumber(i)
      case l: java.lang.Long    => JsNumber(l)
      case b: java.lang.Boolean => JsBoolean(b)
      case s: java.lang.String  => JsString(s)
      case _                    => JsString(obj.toString)
    }
    override def read(json: JsValue): AnyRef = json match {
      case JsNumber(n)       => n.intValue().asInstanceOf[AnyRef]
      case JsString(s)       => s
      case JsBoolean(b)      => b.asInstanceOf[AnyRef]
      case JsArray(elements) => elements
    }
  }

  implicit def sResponseFmt[R: JsonFormat] = jsonFormat2(SuccessfulResponse.apply[R])
  implicit val execTimeFmt = jsonFormat2(ExecInfo.apply)
  implicit val finishedJobResponseFmt = jsonFormat2(FinishedJobResponse.apply)

  implicit val fResponseFmt = jsonFormat3(FailureResponse.apply)

  implicit val jdbcInpConfFmt = jsonFormat(
    JDBCInputConf.apply,
    "sourceId",
    "jdbcUrl",
    "query",
    "driverName",
    "datetimeField",
    "eventsMaxGapMs",
    "defaultEventsGapMs",
    "partitionFields",
    "userName",
    "password",
    "props",
    "parallelism",
    "patternsParallelism",
    "sinkParallelism"
  )
  implicit val influxInpConfFmt = jsonFormat(
    InfluxDBInputConf.apply,
    "sourceId",
    "dbName",
    "url",
    "query",
    "eventsMaxGapMs",
    "defaultEventsGapMs",
    "partitionFields",
    "datetimeField",
    "userName",
    "password",
    "timeoutSec",
    "parallelism",
    "patternsParallelism",
    "sinkParallelism"
  )

  implicit val rowSchemaFmt = jsonFormat(
    RowSchema.apply,
    "sourceIdField",
    "fromTsField",
    "toTsField",
    "appIdFieldVal",
    "patternIdField",
    "processingTsField",
    "contextField",
    "forwardedFields"
  )
  // implicit val jdbcSinkSchemaFmt = jsonFormat(JDBCSegmentsSink.apply, "tableName", "rowSchema")
  implicit val jdbcOutConfFmt = jsonFormat7(JDBCOutputConf.apply)

  implicit val rawPatternFmt = jsonFormat4(RawPattern.apply)
  implicit def patternsRequestFmt[IN <: InputConf[_]: JsonFormat, OUT <: OutputConf[_]: JsonFormat] =
    jsonFormat4(FindPatternsRequest.apply[IN, OUT])
  implicit val dslPatternFmt = jsonFormat1(DSLPatternRequest.apply)
}
