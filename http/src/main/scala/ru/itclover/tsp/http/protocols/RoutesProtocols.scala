package ru.itclover.tsp.http.protocols

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import ru.itclover.tsp.core.RawPattern
import ru.itclover.tsp.http.domain.input.{DSLPatternRequest, FindPatternsRequest, QueueableRequest}
import ru.itclover.tsp.http.domain.output.SuccessfulResponse.ExecInfo
import ru.itclover.tsp.http.domain.output.{FailureResponse, SuccessfulResponse}
import ru.itclover.tsp.streaming.io._
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
      case JsNumber(n)       => n.intValue.asInstanceOf[AnyRef]
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
      case JsNumber(n)       => n.intValue.asInstanceOf[AnyRef]
      case JsString(s)       => s
      case JsBoolean(b)      => b.asInstanceOf[AnyRef]
      case JsArray(elements) => elements
    }

  }

  implicit val execTimeFmt: RootJsonFormat[ExecInfo] = jsonFormat2(ExecInfo.apply)

  implicit def sResponseFmt[R: JsonFormat]: RootJsonFormat[SuccessfulResponse[R]] = jsonFormat2(
    SuccessfulResponse.apply[R]
  )

  implicit val fResponseFmt: RootJsonFormat[FailureResponse] = jsonFormat3(FailureResponse.apply)

  implicit def nduFormat[Event, EKey: JsonFormat, EValue: JsonFormat]
    : RootJsonFormat[NarrowDataUnfolding[Event, EKey, EValue]] =
    jsonFormat(
      NarrowDataUnfolding[Event, EKey, EValue],
      "keyColumn",
      "defaultValueColumn",
      "fieldsTimeoutsMs",
      "valueColumnMapping",
      "defaultTimeout"
    )

  implicit def wdfFormat[Event, EKey: JsonFormat, EValue: JsonFormat]
    : RootJsonFormat[WideDataFilling[Event, EKey, EValue]] =
    jsonFormat(WideDataFilling[Event, EKey, EValue], "fieldsTimeoutsMs", "defaultTimeout")

  implicit def sdtFormat[Event, EKey: JsonFormat, EValue: JsonFormat]
    : RootJsonFormat[SourceDataTransformation[Event, EKey, EValue]] =
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
          case _ => deserializationError("Unknown source data transformation")
        }
        JsObject(
          "type"   -> obj.`type`.toJson,
          "config" -> c
        )
      }

    }

  implicit val jdbcInpConfFmt: RootJsonFormat[JDBCInputConf] = jsonFormat(
    JDBCInputConf.apply,
    "sourceId",
    "jdbcUrl",
    "query",
    "driverName",
    "datetimeField",
    "eventsMaxGapMs",
    "defaultEventsGapMs",
    "chunkSizeMs",
    "processingBatchSize",
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

  implicit val kafkaInpConfFmt: RootJsonFormat[KafkaInputConf] = jsonFormat15(
    KafkaInputConf.apply
  )

  implicit def inpConfFmt[Event, EKey: JsonFormat, EValue: JsonFormat]: RootJsonFormat[InputConf[Event, EKey, EValue]] =
    new RootJsonFormat[InputConf[Event, EKey, EValue]] {

      override def read(json: JsValue): InputConf[Event, EKey, EValue] = json match {
        case obj: JsObject =>
          val tp = obj.fields.getOrElse("type", sys.error("Input (source) config: missing type"))
          val cfg = obj.fields.getOrElse("config", sys.error("Input (source) config: missing config"))
          tp match {
            case JsString("kafka") => kafkaInpConfFmt.read(cfg).asInstanceOf[InputConf[Event, EKey, EValue]]
            case JsString("jdbc")  => jdbcInpConfFmt.read(cfg).asInstanceOf[InputConf[Event, EKey, EValue]]
            case _                 => deserializationError(s"Input (source) config: unknown type $tp")
          }
        case _ =>
          deserializationError(s"Source data transformation must be an object, but got ${json.compactPrint} instead")
      }

      override def write(obj: InputConf[Event, EKey, EValue]): JsValue = {
        val (t, c) = obj match {
          case kafkain: KafkaInputConf => ("kafka", kafkaInpConfFmt.write(kafkain))
          case jdbcin: JDBCInputConf   => ("jdbc", jdbcInpConfFmt.write(jdbcin))
          case _                       => deserializationError("Unknown input (source) config")
        }
        JsObject(
          "type"   -> t.toJson,
          "config" -> c
        )
      }

    }

  implicit val newRowSchemaFmt: RootJsonFormat[NewRowSchema] = jsonFormat1(NewRowSchema.apply)

  implicit val intESValueFormat: RootJsonFormat[IntESValue] = jsonFormat2(IntESValue.apply)
  implicit val floatESValueFormat: RootJsonFormat[FloatESValue] = jsonFormat2(FloatESValue.apply)
  implicit val stringESValueFormat: RootJsonFormat[StringESValue] = jsonFormat2(StringESValue.apply)
  implicit val objectESValueFormat: RootJsonFormat[ObjectESValue] = jsonFormat2(ObjectESValue.apply)

  implicit def eventSchemaValueFormat: RootJsonFormat[EventSchemaValue] = new RootJsonFormat[EventSchemaValue] {

    override def read(json: JsValue): EventSchemaValue = json match {
      case obj: JsObject =>
        val t = obj.fields.getOrElse("type", deserializationError("Event schema field: missing type"))
        val v = obj.fields.getOrElse("value", deserializationError("Event schema field: missing value"))
        val typeName = t match {
          case JsString(value) => value
          case _               => deserializationError(s"Type name must be string, but got `${t.compactPrint}` instead")
        }
        v match {
          case JsObject(fields) =>
            if (typeName != "object")
              deserializationError("Type name for nested structure must be `object`")
            else
              ObjectESValue(typeName, fields.map { case (k, v) => (k, read(v)) })
          case JsArray(_)      => deserializationError("Array values not yet supported")
          case JsString(value) => StringESValue(typeName, value)
          case JsNumber(value) => FloatESValue(typeName, value.floatValue)
          case _: JsBoolean    => deserializationError("Boolean values not yet supported")
          case JsNull          => deserializationError("Null values not supported")
        }
      case _ =>
        deserializationError(s"Event schema field must be an object, but got ${json.compactPrint} instead")
    }

    override def write(obj: EventSchemaValue): JsValue = obj match {
      case IntESValue(t, v)    => JsObject("type" -> t.toJson, "value" -> v.toJson)
      case FloatESValue(t, v)  => JsObject("type" -> t.toJson, "value" -> v.toJson)
      case StringESValue(t, v) => JsObject("type" -> t.toJson, "value" -> v.toJson)
      case ObjectESValue(t, v) =>
        JsObject("type" -> t.toJson, "value" -> v.toJson(mapFormat(StringJsonFormat, eventSchemaValueFormat)))
    }

  }

  implicit object eventSchemaFmt extends JsonFormat[EventSchema] {

    override def read(json: JsValue): EventSchema = Try(newRowSchemaFmt.read(json))
      .getOrElse(deserializationError("Cannot serialize EventSchema"))

    override def write(obj: EventSchema): JsValue = obj match {
      case newRowSchema: NewRowSchema => newRowSchemaFmt.write(newRowSchema)
    }

  }

  // implicit val jdbcSinkSchemaFmt = jsonFormat(JDBCSegmentsSink.apply, "tableName", "rowSchema")
  implicit val jdbcOutConfFmt: RootJsonFormat[JDBCOutputConf] = jsonFormat(
    JDBCOutputConf.apply,
    "tableName",
    "rowSchema",
    "jdbcUrl",
    "driverName",
    "password",
    "batchInterval",
    "userName",
    "parallelism"
  )

  implicit val kafkaOutConfFmt: RootJsonFormat[KafkaOutputConf] = jsonFormat(
    KafkaOutputConf.apply,
    "broker",
    "topic",
    "serializer",
    "rowSchema",
    "parallelism"
  )

  implicit def outConfFmt[Event]: RootJsonFormat[OutputConf[Event]] =
    new RootJsonFormat[OutputConf[Event]] {

      override def read(json: JsValue): OutputConf[Event] = json match {
        case obj: JsObject =>
          val tp = obj.fields.getOrElse("type", sys.error("Output (sink) config: missing type"))
          val cfg = obj.fields.getOrElse("config", sys.error("Output (sink) config: missing config"))
          tp match {
            case JsString("kafka") => kafkaOutConfFmt.read(cfg).asInstanceOf[OutputConf[Event]]
            case JsString("jdbc")  => jdbcOutConfFmt.read(cfg).asInstanceOf[OutputConf[Event]]
            case _                 => deserializationError(s"Output (sink) config: unknown type $tp")
          }
        case _ =>
          deserializationError(s"Source data transformation must be an object, but got ${json.compactPrint} instead")
      }

      override def write(obj: OutputConf[Event]): JsValue = {
        val (t, c) = obj match {
          case kafkaout: KafkaOutputConf => ("kafka", kafkaOutConfFmt.write(kafkaout))
          case jdbcout: JDBCOutputConf   => ("jdbc", jdbcOutConfFmt.write(jdbcout))
          case _                         => deserializationError("Unknown output (sink) config")
        }
        JsObject(
          "type"   -> t.toJson,
          "config" -> c
        )
      }

    }

  implicit val rawPatternFmt: RootJsonFormat[RawPattern] = jsonFormat4(RawPattern.apply)

  implicit def patternsRequestFmt[Event, EKey, EValue, OutEvent](implicit
    inFormat: JsonFormat[InputConf[Event, EKey, EValue]],
    outFormat: JsonFormat[OutputConf[OutEvent]]
  ): RootJsonFormat[FindPatternsRequest[Event, EKey, EValue, OutEvent]] =
    jsonFormat(
      FindPatternsRequest.apply[Event, EKey, EValue, OutEvent],
      "uuid",
      "source",
      "sinks",
      "priority",
      "patterns"
    )

  class QueueableRequestFmt[Event, EKey, EValue, OutEvent](implicit
    inFormat: JsonFormat[InputConf[Event, EKey, EValue]],
    outFormat: JsonFormat[OutputConf[OutEvent]]
  ) extends JsonFormat[QueueableRequest] {

    override def read(json: JsValue): QueueableRequest = patternsRequestFmt[Event, EKey, EValue, OutEvent].read(json)

    override def write(obj: QueueableRequest): JsValue = obj match {
      case x @ FindPatternsRequest(_, _, _, _, _) =>
        patternsRequestFmt[Event, EKey, EValue, OutEvent]
          .write(x.asInstanceOf[FindPatternsRequest[Event, EKey, EValue, OutEvent]])
    }

  }

  implicit def queueableRequestFmt[Event, EKey, EValue, OutEvent](implicit
    inFormat: JsonFormat[InputConf[Event, EKey, EValue]],
    outFormat: JsonFormat[OutputConf[OutEvent]]
  ): JsonFormat[QueueableRequest] = (new QueueableRequestFmt[Event, EKey, EValue, OutEvent])

  implicit val dslPatternFmt: RootJsonFormat[DSLPatternRequest] = jsonFormat1(DSLPatternRequest.apply)

}
