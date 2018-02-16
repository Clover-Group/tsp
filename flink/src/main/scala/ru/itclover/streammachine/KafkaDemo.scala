package ru.itclover.streammachine

import java.util.Properties

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.api.scala._
import org.apache.flink.streaming.util.serialization.{JSONDeserializationSchema, JsonRowDeserializationSchema}
import org.apache.flink.types.Row
import ru.itclover.streammachine.core.PhaseParser.Functions.not
import ru.itclover.streammachine.core.Time.more
import ru.itclover.streammachine.phases.Phases.Phase
import ru.itclover.streammachine.transformers.{FlinkStateCodeMachineMapper, FlinkStateMachineMapper}
import ru.itclover.streammachine.core.Time._
import ru.itclover.streammachine.core._
import ru.itclover.streammachine.core.PhaseParser.Functions._
import ru.itclover.streammachine.phases.NumericPhases.SymbolParser
import ru.itclover.streammachine.phases.NumericPhases._
import scala.concurrent.duration._
import scala.util.Random
import scala.util.matching.Regex

object  KafkaDemo extends App {

  val properties = new Properties()
  properties.setProperty("bootstrap.servers", "10.30.5.229:9092,10.30.5.251:9092")
  properties.setProperty("group.id", "testflink" + Random.nextLong())
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  case class Appevent(eventType: String, created: Long, userId: String)

  val objectMapper = new ObjectMapper() with ScalaObjectMapper
  objectMapper.registerModule(DefaultScalaModule)
  objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  val javaType = objectMapper.constructType[Appevent]

  //  val eventTypes = Set("TABLE_JOIN", "BET_ACCEPTED", "BET_REQUESTED")
  val eventTypes = Set("GHA_REQUEST", "GAME_BETS_CLOSED", "GAME_BETS_OPEN")

  implicit val symbolNumberExtractorAppevent: SymbolExtractor[Appevent, String] = new SymbolExtractor[Appevent, String] {
    override def extract(event: Appevent, symbol: Symbol): String = {
      symbol match {
        case 'eventType => event.eventType
        case _ => sys.error(s"No field $symbol in $event")
      }
    }
  }

  implicit val extractTimeAppevent: TimeExtractor[Appevent] = new TimeExtractor[Appevent] {
    override def apply(v1: Appevent): Time = v1.created
  }


  val phaseParser: Phase[Appevent] =
    ('eventType.as[String] === "GAME_BETS_OPEN")
      .andThen(
        not(
          ('eventType.as[String] === "GAME_BETS_CLOSED")
            or
            ('eventType.as[String] === "BET_REQUESTED")
        ).timed(less(30.seconds))
      )

  val stream = env
    .addSource(new FlinkKafkaConsumer010[String]("common", new SimpleStringSchema(), properties))
    .map(str => objectMapper.readValue[Appevent](str, javaType))
    //      .assignTimestampsAndWatermarks()
    .filter(e => eventTypes(e.eventType))
    .filter(_.userId != null)
    .keyBy(_.userId)
    .flatMap(FlinkStateMachineMapper(phaseParser.mapWithEvent{case (a,b) => a.userId -> b})(FakeMapper()))
    .print()


  env.execute()
}
