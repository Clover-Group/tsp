package ru.itclover.tsp.io.output

import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.types.Row

trait OutputConf[Event] {
  def forwardedFields: Seq[Symbol]

  def getOutputFormat: OutputFormat[Event]

  def parallelism: Option[Int]
}


case class JDBCOutputConf(tableName: String,
                          rowSchema: RowSchema,
                          jdbcUrl: String,
                          driverName: String,
                          password: Option[String] = None,
                          batchInterval: Option[Int] = None,
                          userName: Option[String] = None,
                          parallelism: Option[Int] = Some(1)) extends OutputConf[Row] {
  override def getOutputFormat = JDBCOutput.getOutputFormat(this)

  override def forwardedFields = rowSchema.forwardedFields
}