package ru.itclover.streammachine

import java.time.Instant

import akka.actor.FSM.Failure
import org.apache.flink.api.common.operators.GenericDataSinkBase
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.core.fs.{FileSystem, Path}
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.types.Row
import ru.itclover.streammachine.core.PhaseResult.Success
import ru.itclover.streammachine.io.input.source.JDBCSourceInfo
import ru.itclover.streammachine.io.input.{JDBCInputConf => InpJDBCConfig}
import ru.itclover.streammachine.io.output.{ClickhouseOutput, JDBCSegmentsSink, JDBCOutputConf => OutJDBCConfig}

import scala.collection.immutable.SortedMap
//import ru.itclover.streammachine.io.input.{ClickhouseInput, KafkaInput}



object IoDemo {

  case class Row2(time: Instant, speedEngine: Int, contuctorOilPump: Int, wagonId: Int)

  def main(args: Array[String]): Unit = {

    case class Temp(wagon: Int, datetime: String, temp: Float)

    import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
    import org.apache.flink.api.scala._

    val streamEnv = StreamExecutionEnvironment.createLocalEnvironment()

    val inpConfig = InpJDBCConfig(
      jdbcUrl = "jdbc:clickhouse://localhost:8123/renamedTest",
      query = "select Wagon_id, datetime, Tin_1 from series765_data limit 110100, 4000000",
      driverName = "ru.yandex.clickhouse.ClickHouseDriver",
      datetimeColname = 'datetime,
      partitionColnames = Seq('Wagon_id)
    )
    val srcInfo = JDBCSourceInfo(inpConfig) match {
      case Right(srcInfo) => srcInfo
      case Left(err) => throw err
    }

    val dataStream = streamEnv.createInput(srcInfo.inputFormat)
    val ds = dataStream.map(row => Temp(row.getField(0).asInstanceOf[Int], row.getField(1).asInstanceOf[String],
      row.getField(2).asInstanceOf[Float]))


//    val outConfig = OutJDBCConfig(
//      jdbcUrl = "jdbc:clickhouse://localhost:8123/renamedTest",
//      sinkSchema = JDBCSinkSchema("series765_data_sink"),
//      driverName = "ru.yandex.clickhouse.ClickHouseDriver",
//      batchInterval = Some(1000000)
//    )
//    val chOutputFormat = ClickhouseOutput.getOutputFormat(outConfig)
//
//    class DictMapFunction(val sinkColumnsNames: List[Symbol]) extends RichMapFunction[Map[Symbol, Any], Row] {
//      assert(sinkColumnsNames.nonEmpty, "Cannot map out format - out columns not defined.")
//
//      val rowSize: Int = sinkColumnsNames.size
//
//      def map(inItem: Map[Symbol, Any]): Row = {
//        println(inItem)
//        val row = new Row(rowSize)
//        for ((columnName, colIndex) <- sinkColumnsNames.zip(0 until rowSize)) {
//          row.setField(colIndex, inItem(columnName))
//        }
//        row
//      }
//    }
//
//    def productToMap(cc: Product, namesMap: Map[Symbol, Symbol]) = {
//      // TODO Rm symbol?
//      val values = cc.productIterator
//      cc.getClass.getDeclaredFields.map(f => namesMap(Symbol(f.getName)) -> values.next).toMap
//    }

//    // TODO Mb to sink function: ds.addSink(new JDBCSinkFunction(chOutputFormat))
//    val outStream = ds.map(x => productToMap(x, Map('wagon -> 'Wagon_id, 'datetime -> 'datetime, 'temp -> 'Tin_1)))
//                      .map(x => new DictMapFunction(outConfig.sinkColumnsNames).map(x))
//    outStream.writeUsingOutputFormat(chOutputFormat)
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
