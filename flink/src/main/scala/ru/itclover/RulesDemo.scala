package ru.itclover

import java.text.SimpleDateFormat
import java.time.Instant

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.io.{RowCsvInputFormat, TupleCsvInputFormat}
import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import ru.itclover.streammachine.phases.Phases.{Assert, Decreasing, Timer, Wait}


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

    //"2017-09-13 10:00:00"
    val format = new SimpleDateFormat("\"yyyy-MM-dd HH:mm:ss\"")

    import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

    val tupleTypeInfoBase = implicitly[TypeInformation[(String, Int, Int, Int)]].asInstanceOf[CaseClassTypeInfo[(String, Int, Int, Int)]]

    val streamEnv = StreamExecutionEnvironment.createLocalEnvironment()

    val dataSet = streamEnv.readFile(
      new TupleCsvInputFormat[(String, Int, Int, Int)](filePath, tupleTypeInfoBase),
      args(0),
      FileProcessingMode.PROCESS_ONCE, 100)


    val rows = dataSet.map(r => Row(format.parse(r._1).toInstant, r._2, r._3, r._4))

    //    +1_____________Остановка без прокачки масла
    //    1. начало куска ContuctorOilPump не равен 0 И SpeedEngine уменьшается с 260 до 0
    //    2. конец когда SpeedEngine попрежнему 0 и ContactorOilPump = 0 и между двумя этими условиями прошло меньше 60 сек
    //      """ЕСЛИ SpeedEngine перешел из "не 0" в "0" И в течение 90 секунд суммарное время когда ContactorBlockOilPumpKMN = "не 0" менее 60 секунд"""
    val phase =
    ((Assert[Row](_.contuctorOilPump != 0) & Decreasing(_.speedEngine, 260, 0))
      .andThen(Assert(_.speedEngine == 0)) &
      (Wait[Row](_.contuctorOilPump == 0) & Timer(_.time, atMaxSeconds = 60)))
      .map{
        case (_, (condition, (start, end)))=> ""
      }

    val alerts = rows
      // partition on the address to make sure equal addresses
      // end up in the same state machine flatMap function
      .keyBy(_.wagonId)
      // the function that evaluates the state machine over the sequence of events
      .flatMap(StateMachineMapper(phase))


    //    alerts.print()
    alerts.writeAsCsv("/tmp/output.csv")

    //    alerts.writeAsCsv("/tmp/output.csv")

    //      // output to standard-out
    //      .print()

    // trigger program execution
    streamEnv.execute()
  }
}