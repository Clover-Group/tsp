package ru.itclover.streammachine.http.routes


import java.util.concurrent.TimeUnit
import akka.actor.ActorSystem
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import akka.stream.ActorMaterializer
import akka.http.scaladsl.model.StatusCodes._
import com.typesafe.scalalogging.Logger
import org.apache.flink.api.scala._
import ru.itclover.streammachine.http.domain.input.FindPatternsRequest
import ru.itclover.streammachine.http.domain.output.{FailureResponse, SuccessfulResponse}
import ru.itclover.streammachine.http.protocols.JsonProtocols
import ru.itclover.streammachine.io.input.{InputConf, JDBCInputConf}
import ru.itclover.streammachine.io.output.{JDBCOutput, JDBCOutputConf}
import ru.itclover.streammachine.transformers.{PatternsSearchStages, SparseRowsDataAccumulator, StreamSources}
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import cats.data.Reader
import ru.itclover.streammachine.http.services.kafka.HttpSchemaRegistryClient
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010
import ru.itclover.streammachine.utils.Time.timeIt
import scala.concurrent.duration._
import ru.itclover.streammachine.http.utils.ImplicitUtils.RightBiasedEither
import ru.itclover.streammachine.serializers.RowAvroSerializer
import scala.util.{Failure, Success}


object JdbcToKafkaStreamRoute {
  def fromExecutionContext(implicit strEnv: StreamExecutionEnvironment): Reader[ExecutionContextExecutor, Route] =
    Reader { execContext =>
      new JdbcToKafkaStreamRoute {
        override implicit val executionContext: ExecutionContextExecutor = execContext
        override implicit val streamEnv: StreamExecutionEnvironment = strEnv
      }.route
    }
}


trait JdbcToKafkaStreamRoute extends JsonProtocols {
  implicit val system: ActorSystem = ActorSystem("my-system")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher
  implicit val streamEnv = StreamExecutionEnvironment.createLocalEnvironment()

  private val log = Logger[JdbcStreamRoutes]

  val route: Route = path("streamJob" / "from-jdbc" / "to-kafka" /) {
    entity(as[FindPatternsRequest[JDBCInputConf, JDBCOutputConf]]) { patternsRequest =>
      val (inputConf, outputConf, patterns) = (patternsRequest.source, patternsRequest.sink, patternsRequest.patterns)
      log.info(s"Starting patterns finding with input JDBC conf: `$inputConf`,\nOutput Kafka conf: `$outputConf`\n" +
        s"patterns codes: `$patterns`")

      val jobIdOrError = for {
        stream <- StreamSources.fromJdbc(inputConf)
        patterns <- PatternsSearchStages.findInRows(stream, inputConf, patterns, outputConf.rowSchema)
      } yield {
        val schemaUri = "http://localhost:8081/schemas/ids/1"
        HttpSchemaRegistryClient().getSchema(schemaUri) map { schema =>
          val producer = new FlinkKafkaProducer010("localhost:9092", "test", RowAvroSerializer(Map('f1 -> 0), schema))
          patterns.addSink(producer).name(s"Writing patterns to Kafka")
          streamEnv.execute()
        }
      }

      jobIdOrError match {
        case Right(job) => onComplete(job) {
          case Success(jobResult) => {
            val execTimeLog = s"Job execution time - ${jobResult.getNetRuntime(TimeUnit.SECONDS)}sec"
            complete(SuccessfulResponse(jobResult.hashCode, Seq(execTimeLog)))
          }
          case Failure(err) => complete(InternalServerError, FailureResponse(5005, err))
        }
        case Left(err) => complete(InternalServerError, FailureResponse(5004, err))
      }
    }
  }
}
