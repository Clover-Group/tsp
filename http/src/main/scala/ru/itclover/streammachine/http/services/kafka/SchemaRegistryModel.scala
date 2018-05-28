package ru.itclover.streammachine.http.services.kafka

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json.{DefaultJsonProtocol, _}
import akka.http.scaladsl.model.{HttpEntity, _}
import akka.http.scaladsl.unmarshalling.{Unmarshal, Unmarshaller}
import akka.stream.{ActorMaterializer, Materializer}
import ru.itclover.streammachine.http.services.kafka.SchemaRegistryModel._
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.language.implicitConversions


object SchemaRegistryModel {

  type RawSchema = String

  type ErrorOrSchema = Either[SchemaRegError, RawSchema]

  case class SchemaRegError(errorCode: Int, message: String)

  final case class SchemaID private(value: Int) extends AnyVal
  object SchemaID {
    implicit def fromInt(id: Int) = SchemaID(id)
  }

  trait SchemaException extends Product with Serializable

  case class AvroException(err: String) extends Exception with SchemaException

  case class SchemaBuilderException(err: String) extends Exception with SchemaException
}


trait AvroJsonProtocols extends SprayJsonSupport with DefaultJsonProtocol {

  val `application/schema+json` = MediaType.custom("application/vnd.schemaregistry.v1+json", binary=false)
  val schemaContentType: ContentType = ContentType(`application/schema+json`, () => HttpCharsets.`UTF-8`)
  val sRegistryResponseTimeout: FiniteDuration = 10.seconds

  implicit val schemaRegErrFmt = jsonFormat(SchemaRegError.apply, "error_code", "message")
  implicit val schemaRegPutSchemaFmt = jsonFormat1(SchemaID.apply)

  implicit val rawSchemaUnmarshaller = Unmarshaller.withMaterializer[HttpEntity, RawSchema] {
    implicit execCtx: ExecutionContext => implicit mat: Materializer => rawEntity: HttpEntity => {
        rawEntity.withContentType(schemaContentType).toStrict(sRegistryResponseTimeout) flatMap { entity =>
          val dataStr = entity.getData.utf8String
          def mustBeObjErr(schema: String) = s"Invalid schema format - must be an JSON object: `$schema`"
          Future {
            dataStr.parseJson.asJsObject(mustBeObjErr(dataStr)).getFields("schema").headOption match {
              case Some(JsString(schemaStr)) => schemaStr
              case Some(s) =>
                throw new ParsingException(ErrorInfo(mustBeObjErr(s.compactPrint)))
              case None =>
                throw new ParsingException(ErrorInfo(s"Invalid schema format - not contain `schema` key: `$dataStr`"))
            }
          }
        }
      }
  }

  implicit val schemaIdUnmarshaller = Unmarshaller.withMaterializer[HttpEntity, SchemaID] {
    implicit execCtx: ExecutionContext => implicit mat: Materializer => rawEntity: HttpEntity => {
      rawEntity.withContentType(schemaContentType).toStrict(sRegistryResponseTimeout) flatMap { entity =>
        entity.getData.utf8String.parseJson.asJsObject().getFields("id").headOption match {
          case Some(JsNumber(id)) =>
            Future.successful(id.toInt)
          case _ =>
            Future.failed(AvroException("POST request for the schema creation is't return any id."))
        }
      }
    }
  }

  implicit val errorUnmarshaller = Unmarshaller.withMaterializer[HttpEntity, SchemaRegError] {
    implicit execCtx: ExecutionContext => implicit mat: Materializer => rawEntity: HttpEntity =>
        rawEntity.withContentType(schemaContentType).toStrict(sRegistryResponseTimeout) map {
          _.getData.utf8String.parseJson.convertTo[SchemaRegError]
        }
  }

  implicit val schemaOrErrorUmn = Unmarshaller.eitherUnmarshaller[SchemaRegError, RawSchema]
}
