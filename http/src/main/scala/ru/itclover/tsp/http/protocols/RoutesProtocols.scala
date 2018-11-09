package ru.itclover.tsp.http.protocols

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import ru.itclover.tsp.dsl.schema.RawPattern
import ru.itclover.tsp.http.domain.input.{DSLPatternRequest, FindPatternsRequest}
import ru.itclover.tsp.http.domain.output.{ExecInfo, FailureResponse, FinishedJobResponse, SuccessfulResponse}
import ru.itclover.tsp.io.input._
import ru.itclover.tsp.io.output.{JDBCOutputConf, OutputConf, RowSchema}
import spray.json._

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
  implicit val nduFormat = jsonFormat(NarrowDataUnfolding.apply, "key", "value", "fieldsTimeoutsMs", "defaultTimeout")
  implicit val wdfFormat = jsonFormat(WideDataFilling, "fieldsTimeoutsMs", "defaultTimeout")
  implicit val sdtFormat = new RootJsonFormat[SourceDataTransformation] {
    override def read(json: JsValue): SourceDataTransformation = json match {
      case obj: JsObject =>
        val tp = obj.fields.getOrElse("type", sys.error("Source data transformation: missing type"))
        val cfg = obj.fields.getOrElse("config", sys.error("Source data transformation: missing config"))
        tp match {
          case JsString("NarrowDataUnfolding") => nduFormat.read(cfg)
          case JsString("WideDataFilling")   => wdfFormat.read(cfg)
          case _                               => sys.error(s"Source data transformation: unknown type $tp")
        }
      case _ => sys.error(s"Source data transformation must be an object, but got ${json.compactPrint} instead")
    }
    override def write(obj: SourceDataTransformation): JsValue = {
      val c = obj.config match {
        case ndu: NarrowDataUnfolding => ndu.toJson
        case wdf: WideDataFilling     => wdf.toJson
        case _                        => sys.error("Unknown source data transformation")
      }
      JsObject(
        "type"   -> obj.`type`.toJson,
        "config" -> c
      )
    }
  }

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
    "dataTransformation",
    "parallelism",
    "numParallelSources",
    "patternsParallelism"
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
    "dataTransformation",
    "parallelism",
    "numParallelSources",
    "patternsParallelism"
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
  implicit val jdbcOutConfFmt = jsonFormat8(JDBCOutputConf.apply)

  implicit val rawPatternFmt = jsonFormat4(RawPattern.apply)
  implicit def patternsRequestFmt[IN <: InputConf[_]: JsonFormat, OUT <: OutputConf[_]: JsonFormat] =
    jsonFormat4(FindPatternsRequest.apply[IN, OUT])
  implicit val dslPatternFmt = jsonFormat1(DSLPatternRequest.apply)
}
