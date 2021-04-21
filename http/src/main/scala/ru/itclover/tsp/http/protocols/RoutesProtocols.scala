package ru.itclover.tsp.http.protocols

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import ru.itclover.tsp.core.RawPattern
import ru.itclover.tsp.http.domain.input.{DSLPatternRequest, FindPatternsRequest}
import ru.itclover.tsp.http.domain.output.SuccessfulResponse.ExecInfo
import ru.itclover.tsp.http.domain.output.{FailureResponse, SuccessfulResponse}
import spray.json._
import ru.itclover.tsp.spark
import ru.itclover.tsp.spark.io.{
  SourceDataTransformation => SparkSDT,
  NarrowDataUnfolding => SparkNDU,
  WideDataFilling => SparkWDF
}

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

  implicit val rawPatternFmt = jsonFormat3(RawPattern.apply)

  // TODO: Remove type bounds for (In|Out)putConf?
  implicit def sparkPatternsRequestFmt[IN <: spark.io.InputConf[_, _, _]: JsonFormat, OUT <: spark.io.OutputConf[_]: JsonFormat] =
    jsonFormat(FindPatternsRequest.apply[IN, OUT], "uuid", "source", "sink", "patterns")

  implicit val dslPatternFmt = jsonFormat1(DSLPatternRequest.apply)

  implicit def sparkNduFormat[Event, EKey: JsonFormat, EValue: JsonFormat] =
    jsonFormat(
      SparkNDU[Event, EKey, EValue],
      "keyColumn",
      "defaultValueColumn",
      "fieldsTimeoutsMs",
      "valueColumnMapping",
      "defaultTimeout"
    )
  implicit def sparkWdfFormat[Event, EKey: JsonFormat, EValue: JsonFormat] =
    jsonFormat(SparkWDF[Event, EKey, EValue], "fieldsTimeoutsMs", "defaultTimeout")

  implicit def sparkSdtFormat[Event, EKey: JsonFormat, EValue: JsonFormat] =
    new RootJsonFormat[SparkSDT[Event, EKey, EValue]] {
      override def read(json: JsValue): SparkSDT[Event, EKey, EValue] = json match {
        case obj: JsObject =>
          val tp = obj.fields.getOrElse("type", sys.error("Source data transformation: missing type"))
          val cfg = obj.fields.getOrElse("config", sys.error("Source data transformation: missing config"))
          tp match {
            case JsString("NarrowDataUnfolding") => sparkNduFormat[Event, EKey, EValue].read(cfg)
            case JsString("WideDataFilling")     => sparkWdfFormat[Event, EKey, EValue].read(cfg)
            case _                               => deserializationError(s"Source data transformation: unknown type $tp")
          }
        case _ =>
          deserializationError(s"Source data transformation must be an object, but got ${json.compactPrint} instead")
      }
      override def write(obj: SparkSDT[Event, EKey, EValue]): JsValue = {
        val c = obj.config match {
          case ndu: SparkNDU[Event, EKey, EValue] => sparkNduFormat[Event, EKey, EValue].write(ndu)
          case wdf: SparkWDF[Event, EKey, EValue] => sparkWdfFormat[Event, EKey, EValue].write(wdf)
          case _                                  => deserializationError("Unknown source data transformation")
        }
        JsObject(
          "type"   -> obj.`type`.toJson,
          "config" -> c
        )
      }
    }

  implicit val sparkRowSchemaFmt = jsonFormat(
    spark.io.RowSchema.apply,
    "unitIdField",
    "fromTsField",
    "toTsField",
    "appIdFieldVal",
    "patternIdField",
    "subunitIdField"
  )

  implicit val sparkJdbcInpConfFmt = jsonFormat(
    spark.io.JDBCInputConf.apply,
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

  implicit val sparkKafkaInpConfFmt = jsonFormat16(
    spark.io.KafkaInputConf.apply
  )

  implicit val sparkJdbcOutConfFmt = jsonFormat8(spark.io.JDBCOutputConf.apply)
  implicit val sparkKafkaOutConfFmt = jsonFormat4(spark.io.KafkaOutputConf.apply)

}
