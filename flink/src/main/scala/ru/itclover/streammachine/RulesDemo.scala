package ru.itclover.streammachine

import java.text.SimpleDateFormat
import java.time.Instant

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.TupleCsvInputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.types.Row
import ru.itclover.streammachine.io.input.{ClickhouseInput, JDBCConfig}
//import ru.itclover.streammachine.io.input.{ClickhouseInput, KafkaInput}
import ru.itclover.streammachine.phases.Phases._

/**
  * Demo streaming program that receives (or generates) a stream of events and evaluates
  * a state machine (per originating IP address) to validate that the events follow
  * the state machine's rules.
  */
object RulesDemo {

  case class Row2(time: Instant, speedEngine: Int, contuctorOilPump: Int, wagonId: Int)

  def main(args: Array[String]): Unit = {

    if (args.isEmpty || args.size > 1) {
      println("Usage: /path/to/csv/file")
      sys.exit(1)
    }

    val filePath = new Path(args(0))

    import org.apache.flink.api.scala._


    //    //"2017-09-13 10:00:00"
    //    val format = new SimpleDateFormat("\"yyyy-MM-dd HH:mm:ss\"")
    //
    import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
    //
    //    val tupleTypeInfoBase = implicitly[TypeInformation[(String, Int, Int, Int)]].asInstanceOf[CaseClassTypeInfo[(String, Int, Int, Int)]]
    //
    val streamEnv = StreamExecutionEnvironment.createLocalEnvironment()
    //
    //    val dataSet = streamEnv.readFile(
    //      new TupleCsvInputFormat[(String, Int, Int, Int)](filePath, tupleTypeInfoBase),
    //      args(0),
    //      FileProcessingMode.PROCESS_ONCE, 100)


    import org.apache.flink.api.scala._
    case class Temp(wagon: Int, datetime: String, temp: Float)

    implicit val rowti: RowTypeInfo = new RowTypeInfo(TypeInformation.of(classOf[Int]), TypeInformation.of(classOf[String]), TypeInformation.of(classOf[Float]))

    val dataStream = streamEnv.createInput(ClickhouseInput.getSource(
      JDBCConfig(
        jdbcUrl = "jdbc:clickhouse://82.202.237.34:8123/renamed",
        query = "select Wagon_id, datetime, Tin_1 from series765_data limit 10000, 100",
        driverName = "ru.yandex.clickhouse.ClickHouseDriver"
      ),
      rowti
    ))

    val ds = dataStream.map(row => Temp(row.getField(0).asInstanceOf[Int], row.getField(1).asInstanceOf[String], row.getField(2).asInstanceOf[Float]))

    ds.writeAsText("/tmp/output.csv")

    //    val rows = dataSet.map(r => Row(format.parse(r._1).toInstant, r._2, r._3, r._4))
    //
    //    val alerts = rows
    //       partition on the address to make sure equal addresses
    //       end up in the same state machine flatMap function
    //      .keyBy(_.wagonId)
    //       the function that evaluates the state machine over the sequence of events
    //      .flatMap(FlinkStateMachineMapper(Rules.stopWithoutOilPumping))
    //
    //    alerts.writeAsCsv("/tmp/output.csv")

    streamEnv.execute()
  }
}
