/*
package ru.itclover.streammachine

import java.time.Instant
import java.util.Date
import javassist.bytecode.stackmap.TypeTag

import akka.actor.FSM.Failure
import org.apache.flink.api.common.operators.GenericDataSinkBase
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.functions.util.ListCollector
import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.{FileSystem, Path}
import org.apache.flink.streaming.api.functions.sink.{OutputFormatSinkFunction, SinkFunction}
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import ru.itclover.streammachine.core.PhaseParser
import ru.itclover.streammachine.core.PhaseResult.Success
import ru.itclover.streammachine.core.Time.TimeExtractor
import ru.itclover.streammachine.io.input.{JDBCInputConfig => InpJDBCConfig}
import ru.itclover.streammachine.io.output.{ClickhouseOutput, JDBCOutputConfig => OutJDBCConfig}
import ru.itclover.streammachine.phases.Phases.{Assert, Decreasing}
import ru.itclover.streammachine.transformers.FlinkStateMachineMapper

import scala.collection.immutable.SortedMap
//import ru.itclover.streammachine.io.input.{ClickhouseInput, KafkaInput}



object RulesDemo {

  case class Row2(time: Instant, speedEngine: Int, contuctorOilPump: Int, wagonId: Int)

  def main(args: Array[String]): Unit = {

    case class Temp(wagon: Int, datetime: String, temp: Float)

    import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
    import org.apache.flink.api.scala._

    import core.Aggregators._
    import core.AggregatingPhaseParser._
    import ru.itclover.streammachine.core.NumericPhaseParser._
    import Predef.{any2stringadd => _, _}
    // import ru.itclover.streammachine.core.Time._

    val streamEnv = StreamExecutionEnvironment.createLocalEnvironment()

    val inpConfig = InpJDBCConfig(
      jdbcUrl = "jdbc:clickhouse://localhost:8123/renamedTest",
      query = "select date, timestamp, Wagon_id, SpeedEngine, ContuctorOilPump from series765_data_test_speed limit 0, 30000",
      driverName = "ru.yandex.clickhouse.ClickHouseDriver",
      datetimeColname = 'datetime,
      partitionColnames = Seq('Wagon_id)
    )
    val fieldsTypesInfo = JDBCInput.queryFieldsTypeInformation(inpConfig) match {
      case Right(typesInfo) => typesInfo
      case Left(err) => throw err
    }
    val chInputFormat = JDBCInput.getInputFormat(inpConfig, fieldsTypesInfo.toArray)
    //    val fieldsTypesInfoMap = fieldsTypesInfo.map({ case (f, ty) => (Symbol(f), ty) }).toMap

//    implicit val symbolNumberExtractorRow: SymbolNumberExtractor[Row] = new SymbolNumberExtractor[Map[Symbol, Double]] {
//      override def extract(event: Map, symbol: Symbol): Double = event(symbol)
//    }
    implicit val symbolNumberExtractorRow: SymbolNumberExtractor[Row] = new SymbolNumberExtractor[Row] {
      // TODO: Make it serializable
      val fieldsIndexesMap = fieldsTypesInfo.map(_._1).map(Symbol(_)).zipWithIndex.toMap
      override def extract(event: Row, symbol: Symbol): Double = {
        event.getField(fieldsIndexesMap(symbol)).asInstanceOf[Double]
      }
    }

    implicit val timeExtractor: TimeExtractor[Row] = new TimeExtractor[Row] {
      override def apply(v1: Row) = {
        v1.getField(1).asInstanceOf[java.sql.Timestamp]
      }
    }

    type Phase[Event] = PhaseParser[Event, _, _]

    val assertPhase = Assert[Row](event => event.getField(3).asInstanceOf[Float].toDouble > 250)
    val decreasePhase = Decreasing[Row, Double](event => event.getField(3).asInstanceOf[Float].toDouble, 250, 50)

    def fakeMapper[Event, PhaseOut](p: PhaseParser[Event, _, PhaseOut]) = FakeMapper[Event, PhaseOut]()
    def segmentMapper[Event, PhaseOut](p: PhaseParser[Event, _, PhaseOut], te: TimeExtractor[Event]) =
      SegmentResultsMapper[Event, PhaseOut]()(te)

    // TODO: Make it serializable
    val stateMachine = FlinkStateMachineMapper(assertPhase, segmentMapper(assertPhase, timeExtractor))


    val dataStream = streamEnv.createInput(chInputFormat)
    val resultStream = dataStream.keyBy(row => row.getField(2)).flatMap(stateMachine)
    //      .map({ f =>
    //      f match {
    //        case (Segment(from, to), _) => write to db
    //        case (_, _) => raise
    //      }
    //    })

    resultStream.map(result => println(s"R = $result"))

//    val outConfig = OutJDBCConfig(
//      jdbcUrl = "jdbc:clickhouse://localhost:8123/renamedTest",
//      sinkTable = "series765_data_sink_test_speed",
//      sinkColumnsNames = List[Symbol]('if_rule_success),
//      driverName = "ru.yandex.clickhouse.ClickHouseDriver",
//      batchInterval = Some(1000)
//    )
//
//    val chOutputFormat = ClickhouseOutput.getOutputFormat(outConfig)
//    val sink = new OutputFormatSinkFunction(chOutputFormat)
//    resultStream.map(res => {
//      val r = new Row(1)
//      r.setField(0, true)
//      r
//    }).addSink(sink)
////    resultStream.map(res => {
////      val r = new Row(1)
////      r.setField(0, true)
////      r
////    }).writeUsingOutputFormat(chOutputFormat)
//
//
//    val t0 = System.nanoTime()
//    println("Strart timer")
//
//    streamEnv.execute()
//
//    val t1 = System.nanoTime()
//    println("Elapsed time: " + (t1 - t0) / 1000000000.0 + " seconds")
  }
}
*/
