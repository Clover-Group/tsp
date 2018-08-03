package ru.itclover.streammachine.io.output

import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.types.Row

trait OutputConf[Event] {
  def getOutputFormat: OutputFormat[Event]
}


case class JDBCOutputConf(tableName: String,
                          rowSchema: RowSchema,
                          jdbcUrl: String,
                          driverName: String,
                          password: Option[String] = None,
                          batchInterval: Option[Int] = None,
                          userName: Option[String] = None) extends OutputConf[Row] {
  override def getOutputFormat = JDBCOutput.getOutputFormat(this)
}