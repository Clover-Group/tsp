package ru.itclover.streammachine

import java.time.Instant

import org.apache.flink.api.common.functions.RichMapFunction
import ru.itclover.streammachine.io.input.source.JDBCSourceInfo
import ru.itclover.streammachine.io.input.{JDBCInputConfig => InpJDBCConfig}
import ru.itclover.streammachine.io.output.{ClickhouseOutput, JDBCSegmentsSink, JDBCOutputConfig => OutJDBCConfig}


object ClickhouseIoDemo {

  def main(args: Array[String]): Unit = {
    import org.apache.flink.api.scala._
    import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

    val streamEnv = StreamExecutionEnvironment.createLocalEnvironment()

    val inpConfig = InpJDBCConfig(
      jdbcUrl = "jdbc:clickhouse://localhost:8123/renamedTest",
      query = "select Wagon_id, datetime, Tin_1 from series765_data limit 110100, 400",
      driverName = "ru.yandex.clickhouse.ClickHouseDriver",
      datetimeColname = 'datetime,
      partitionColnames = Seq('Wagon_id)
    )
    val srcInfo = JDBCSourceInfo(inpConfig) match {
      case Right(srcInfo) => srcInfo
      case Left(err) => throw err
    }

    val dataStream = streamEnv.createInput(srcInfo.inputFormat)

    // TODO: Separate config for generic sink schema
    /*val outConfig = OutJDBCConfig(
      jdbcUrl = "jdbc:clickhouse://localhost:8123/renamedTest",
      sinkSchema = JDBCSinkSchema("series765_data_sink", '),
      driverName = "ru.yandex.clickhouse.ClickHouseDriver",
      batchInterval = Some(1000000)
    )
    val chOutputFormat = ClickhouseOutput.getOutputFormat(outConfig)

    dataStream.writeUsingOutputFormat(chOutputFormat)

    val t0 = System.nanoTime()
    println("Start timer")

    streamEnv.execute()

    println("Elapsed time: " + (System.nanoTime() - t0) / 1000000000.0 + " seconds")*/
  }
}
