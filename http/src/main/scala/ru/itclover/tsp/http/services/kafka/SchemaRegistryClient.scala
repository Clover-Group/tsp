package ru.itclover.tsp.http.services.kafka

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json.{DefaultJsonProtocol, _}
import akka.http.scaladsl.model.{HttpEntity, _}
import akka.http.scaladsl.unmarshalling.{Unmarshal, Unmarshaller}
import akka.stream.{ActorMaterializer, Materializer}
import akka.util.ByteString
import org.apache.avro.Schema
import ru.itclover.tsp.http.services.kafka.SchemaRegistryModel._
import scala.concurrent.duration._
import org.apache.avro.Schema.Parser
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}


trait SchemaRegistryClient {
  def getSchema(uri: Uri): Future[Schema]
  def postSchema(uri: Uri, schema: ByteString): Future[SchemaID]
}


case class HttpSchemaRegistryClient()(implicit as: ActorSystem, ec: ExecutionContextExecutor, mat: ActorMaterializer)
  extends SchemaRegistryClient with AvroJsonProtocols {

  override def getSchema(uri: Uri) = {
    val rawSchema = Http().singleRequest(HttpRequest(uri=uri))
    val schema = rawSchema flatMap { rs => Unmarshal(rs.entity).to[Either[SchemaRegError, RawSchema]] }
    schema map {
      case Right(s) => new Parser().parse(s)
      case Left(error) => throw AvroException(error.toString)
    }
  }

  override def postSchema(uri: Uri, schema: ByteString) = {
    val entity = HttpEntity(schemaContentType, schema)
    Http().singleRequest(HttpRequest(uri=uri, entity=entity, method=HttpMethods.POST)) flatMap {
      case HttpResponse(StatusCodes.Success(_), _, respEntity, _) => Unmarshal(respEntity).to[SchemaID]

      case HttpResponse(_, _, respEntity, _) => Unmarshal(respEntity.withContentType(schemaContentType)).to[SchemaRegError] flatMap (err =>
        Future.failed(AvroException(s"Cannot post schema: ${err.message}, error code: ${err.errorCode}"))
      )
    }
  }
}


case class MockSchemaRegistryClient(returnSchema: Schema)(implicit ec: ExecutionContext) extends SchemaRegistryClient {
  override def getSchema(uri: Uri) = Future { returnSchema }

  override def postSchema(uri: Uri, schema: ByteString) = ???
}
