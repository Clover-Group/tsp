package ru.itclover

import java.text.SimpleDateFormat
import java.time.Instant

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.io.{RowCsvInputFormat, TupleCsvInputFormat}
import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.core.fs.Path
import ru.itclover.streammachine.phases.Phases.{Assert, Decreasing, Timer}


/**
  * Demo streaming program that receives (or generates) a stream of events and evaluates
  * a state machine (per originating IP address) to validate that the events follow
  * the state machine's rules.
  */
object RulesDemo {

  def main(args: Array[String]): Unit = {

    if (args.isEmpty || args.size > 1) {
      println("Usage: /path/to/csv/file")
      sys.exit(1)
    }

    val filePath = new Path(args(0))

    // create the environment to create streams and configure execution
//    val env = ExecutionEnvironment.getExecutionEnvironment

    //datetime,SpeedEngine,ContuctorOilPump,wagon_id
    import org.apache.flink.api.scala._

    //    val dataSet: DataSet[(String, Int, Int, Int)] = env.readCsvFile(filePath)

    case class Row(time: Instant, speedEngine: Int, contuctorOilPump: Int, wagonId: Int)

    //2017-09-13 10:00:00
    val format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")

    import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

    //    val tupleTypeInfoBase = implicitly[TypeInformation[(String, Int, Int, Int)]].asInstanceOf[CaseClassTypeInfo[(String, Int, Int, Int)]]

    val streamEnv = StreamExecutionEnvironment.createLocalEnvironment()

    val dataSet = streamEnv
      .createInput(new RowCsvInputFormat(filePath,
        Array(
          BasicTypeInfo.STRING_TYPE_INFO,
          BasicTypeInfo.INT_TYPE_INFO,
          BasicTypeInfo.INT_TYPE_INFO,
          BasicTypeInfo.INT_TYPE_INFO
        )
      ))

    dataSet.writeAsCsv("/tmp/output.csv")

    val rows = dataSet.map(r =>
      Row(format.parse(r.getField(0).asInstanceOf[String]).toInstant,
      r.getField(1).asInstanceOf[Int],
      r.getField(2).asInstanceOf[Int],
      r.getField(3).asInstanceOf[Int]
    ))
    //
    //    val rows = dataSet.map { line =>
    //      line match {
    //        case (timeString, speed, pump, wagonId) => Row(format.parse(timeString).toInstant, speed, pump, wagonId)
    //      }
    //    }

    val phase =
      Decreasing[Row, Int](_.speedEngine, 250, 50)
        .andThen(
          Assert[Row](_.speedEngine > 0)
//            and
//            Timer[Row](_.time, 10, 30)
        )

    val alerts = rows
      // partition on the address to make sure equal addresses
      // end up in the same state machine flatMap function
      .keyBy(_.wagonId)
      // the function that evaluates the state machine over the sequence of events
      .flatMap(StateMachineMapper(phase))


    //    alerts.print()
    alerts.printToErr()

//    alerts.writeAsCsv("/tmp/output.csv")

    //      // output to standard-out
    //      .print()

    // trigger program execution
    streamEnv.execute()
  }
}