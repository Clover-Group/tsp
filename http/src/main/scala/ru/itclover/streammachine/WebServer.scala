package ru.itclover.streammachine

import java.util.concurrent.TimeUnit

import org.apache.flink.types.Row
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import akka.stream.ActorMaterializer
import ru.itclover.streammachine.core.NumericPhaseParser.PhaseGetter
import ru.itclover.streammachine.core.PhaseParser
import ru.itclover.streammachine.utils.EvalUtils
import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.concurrent.duration.Duration
import scala.io.StdIn


object WebServer extends App with HttpService {
  override val isDebug: Boolean = true

  implicit val system: ActorSystem = ActorSystem("my-system")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  type Phase[Event] = PhaseParser[Event, _, _]

  import ru.itclover.streammachine.core.Aggregators._
  import ru.itclover.streammachine.core.AggregatingPhaseParser._
  import ru.itclover.streammachine.core.NumericPhaseParser._
  import ru.itclover.streammachine.core.Time._
  import Predef.{any2stringadd => _, _}
  import ru.itclover.streammachine.phases.Phases._

//  implicit val symbolNumberExtractorRow: SymbolNumberExtractor[Row] = new SymbolNumberExtractor[Row] {
//    override def extract(event: Row, symbol: Symbol) = {
//      event.getField(fieldsIndexesMap(symbol)).asInstanceOf[Double]
//    }
//  }
//
//  implicit val timeExtractor: TimeExtractor[Row] = new TimeExtractor[Row] {
//    override def apply(v1: Row) = {
//      v1.getField(1).asInstanceOf[java.sql.Timestamp]
//    }
//  }
//
//  object A {
//    def a(): Phase[Row] = 'speed >= 100
//  }

  val getPhase = new com.twitter.util.Eval().apply[() => Phase[Row]]( // Phase[Row]
    """
      |import ru.itclover.streammachine.core.Aggregators._
      |import ru.itclover.streammachine.core.AggregatingPhaseParser._
      |import ru.itclover.streammachine.core.NumericPhaseParser._
      |import ru.itclover.streammachine.core.Time._
      |import Predef.{any2stringadd => _, _}
      |import ru.itclover.streammachine.phases.Phases._
      |import org.apache.flink.types.Row
      |
      |implicit val symbolNumberExtractorRow: SymbolNumberExtractor[Row] = new SymbolNumberExtractor[Row] {
      |  val fieldsIndexesMap: Map[Symbol, Double] = Map.empty // TODO
      |
      |
      |  override def extract(event: Row, symbol: Symbol) = {
      |    event.getField(fieldsIndexesMap(symbol)).asInstanceOf[Double]
      |  }
      |}
      |
      |implicit val timeExtractor: TimeExtractor[Row] = new TimeExtractor[Row] {
      |  override def apply(v1: Row) = {
      |    v1.getField(1).asInstanceOf[java.sql.Timestamp]
      |  }
      |}
      |
      |type Phase[Event] = PhaseParser[Event, _, _]
      |
      |object A {
      | def a(): Phase[Row] = 'speed >= 100
      |}
      |
      |A.a _
    """.stripMargin)


  println(getPhase)

  //  val bindingFuture = Http().bindAndHandle(route, "localhost", 8080)
  //
  //  println(s"Server online at http://localhost:8080/\nPress RETURN to stop...")
  //  StdIn.readLine()
  //  bindingFuture
  //    .flatMap(_.unbind())
  //    .onComplete(_ => system.terminate())
}
