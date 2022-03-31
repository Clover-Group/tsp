package ru.itclover.tsp.http.protocols

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import ru.itclover.tsp.core.RawPattern
import ru.itclover.tsp.http.domain.input.{DSLPatternRequest, FindPatternsRequest, QueueableRequest}
import ru.itclover.tsp.http.domain.output.SuccessfulResponse.ExecInfo
import ru.itclover.tsp.http.domain.output.{FailureResponse, SuccessfulResponse}
import ru.itclover.tsp.io.input._
import ru.itclover.tsp.io.output.{Context, EventSchema, JDBCOutputConf, KafkaOutputConf, NewRowSchema, OutputConf}
import spray.json._

import scala.util.Try

// JsonFormats contain Any fields and converted via asInstanceOf(). Here, it's safe
@SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.AsInstanceOf"))
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

  implicit object anyFormat extends JsonFormat[Any] {
    override def write(obj: Any): JsValue = obj match {
      case i: java.lang.Integer => JsNumber(i)
      case l: java.lang.Long    => JsNumber(l)
      case b: java.lang.Boolean => JsBoolean(b)
      case s: java.lang.String  => JsString(s)
      case _                    => JsString(obj.toString)
    }
    override def read(json: JsValue): Any = json match {
      case JsNumber(n)       => n.intValue().asInstanceOf[AnyRef]
      case JsString(s)       => s
      case JsBoolean(b)      => b.asInstanceOf[AnyRef]
      case JsArray(elements) => elements
    }
  }

  implicit val execTimeFmt = jsonFormat2(ExecInfo.apply)
  implicit def sResponseFmt[R: JsonFormat] = jsonFormat2(SuccessfulResponse.apply[R])

  implicit val fResponseFmt = jsonFormat3(FailureResponse.apply)
  implicit def nduFormat[Event, EKey: JsonFormat, EValue: JsonFormat] =
    jsonFormat(
      NarrowDataUnfolding[Event, EKey, EValue],
      "keyColumn",
      "defaultValueColumn",
      "fieldsTimeoutsMs",
      "valueColumnMapping",
      "defaultTimeout"
    )
  implicit def wdfFormat[Event, EKey: JsonFormat, EValue: JsonFormat] =
    jsonFormat(WideDataFilling[Event, EKey, EValue], "fieldsTimeoutsMs", "defaultTimeout")

  implicit def sdtFormat[Event, EKey: JsonFormat, EValue: JsonFormat] =
    new RootJsonFormat[SourceDataTransformation[Event, EKey, EValue]] {
      override def read(json: JsValue): SourceDataTransformation[Event, EKey, EValue] = json match {
        case obj: JsObject =>
          val tp = obj.fields.getOrElse("type", sys.error("Source data transformation: missing type"))
          val cfg = obj.fields.getOrElse("config", sys.error("Source data transformation: missing config"))
          tp match {
            case JsString("NarrowDataUnfolding") => nduFormat[Event, EKey, EValue].read(cfg)
            case JsString("WideDataFilling")     => wdfFormat[Event, EKey, EValue].read(cfg)
            case _                               => deserializationError(s"Source data transformation: unknown type $tp")
          }
        case _ =>
          deserializationError(s"Source data transformation must be an object, but got ${json.compactPrint} instead")
      }
      override def write(obj: SourceDataTransformation[Event, EKey, EValue]): JsValue = {
        val c = obj.config match {
          case ndu: NarrowDataUnfolding[Event, EKey, EValue] => nduFormat[Event, EKey, EValue].write(ndu)
          case wdf: WideDataFilling[Event, EKey, EValue]     => wdfFormat[Event, EKey, EValue].write(wdf)
          case _                                             => deserializationError("Unknown source data transformation")
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
    "chunkSizeMs",
    "partitionFields",
    "unitIdField",
    "userName",
    "password",
    "dataTransformation",
    "defaultToleranceFraction",
    "parallelism",
    "numParallelSources",
    "patternsParallelism",
    "timestampMultiplier"
  )
  implicit val influxInpConfFmt = jsonFormat(
    InfluxDBInputConf.apply,
    "sourceId",
    "dbName",
    "url",
    "query",
    "eventsMaxGapMs",
    "defaultEventsGapMs",
    "chunkSizeMs",
    "partitionFields",
    "datetimeField",
    "unitIdField",
    "userName",
    "password",
    "timeoutSec",
    "dataTransformation",
    "defaultToleranceFraction",
    "parallelism",
    "numParallelSources",
    "patternsParallelism",
    "additionalTypeChecking"
  )

  implicit val kafkaInpConfFmt = jsonFormat14(
    KafkaInputConf.apply
  )

  implicit val redisConfInputFmt = jsonFormat8(
    RedisInputConf.apply
  )

  implicit val contextFmt = jsonFormat2(Context.apply)

  implicit val newRowSchemaFmt = jsonFormat(
    NewRowSchema.apply,
    "unitIdField",
    "fromTsField",
    "toTsField",
    "appIdFieldVal",
    "patternIdField",
    "subunitIdField",
    "incidentIdField",
    "context"
  )

  implicit object eventSchemaFmt extends JsonFormat[EventSchema] {
    override def read(json: JsValue): EventSchema = Try(newRowSchemaFmt.read(json))
      .getOrElse(deserializationError("Cannot serialize EventSchema"))

    override def write(obj: EventSchema): JsValue = obj match {
      case newRowSchema: NewRowSchema => newRowSchemaFmt.write(newRowSchema)
    }
  }

  // implicit val jdbcSinkSchemaFmt = jsonFormat(JDBCSegmentsSink.apply, "tableName", "rowSchema")
  implicit val jdbcOutConfFmt = jsonFormat8(JDBCOutputConf.apply)

  implicit val kafkaOutConfFmt = jsonFormat5(KafkaOutputConf.apply)

  implicit val rawPatternFmt = jsonFormat5(RawPattern.apply)

  implicit def patternsRequestFmt[IN <: InputConf[_, _, _]: JsonFormat, OUT <: OutputConf[_]: JsonFormat] =
    jsonFormat(FindPatternsRequest.apply[IN, OUT], "uuid", "source", "sink", "priority", "patterns")

  class QueueableRequestFmt[IN <: InputConf[_, _, _]: JsonFormat, OUT <: OutputConf[_]: JsonFormat] extends JsonFormat[QueueableRequest] {
    override def read(json: JsValue): QueueableRequest = patternsRequestFmt[IN, OUT].read(json)

    override def write(obj: QueueableRequest): JsValue = obj match {
      case x @ FindPatternsRequest(_, _, _, _, _) => patternsRequestFmt[IN, OUT]
        .write(x.asInstanceOf[FindPatternsRequest[IN, OUT]])
    }
  }

  implicit def queueableRequestFmt[IN <: InputConf[_, _, _]: JsonFormat, OUT <: OutputConf[_]: JsonFormat]
  : JsonFormat[QueueableRequest] = (new QueueableRequestFmt[IN, OUT])

  // TODO: Remove type bounds for (In|Out)putConf?

  implicit val dslPatternFmt = jsonFormat1(DSLPatternRequest.apply)

}
