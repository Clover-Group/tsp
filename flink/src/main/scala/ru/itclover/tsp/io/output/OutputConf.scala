package ru.itclover.tsp.io.output

import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.types.Row

trait OutputConf[Event] {
  def forwardedFieldsIds: Seq[Symbol]

  def getOutputFormat: OutputFormat[Event]

  def parallelism: Option[Int]
}

/**
  * Sink for anything that support JDBC connection
  * @param rowSchema schema of writing rows, __will be replaced soon__
  * @param jdbcUrl example - "jdbc:clickhouse://localhost:8123/default?"
  * @param driverName example - "ru.yandex.clickhouse.ClickHouseDriver"
  * @param userName for JDBC auth
  * @param password for JDBC auth
  * @param batchInterval batch size for writing found incidents
  * @param parallelism num of parallel task to write data
  */
case class JDBCOutputConf(
  tableName: String,
  rowSchema: RowSchema,
  jdbcUrl: String,
  driverName: String,
  password: Option[String] = None,
  batchInterval: Option[Int] = None,
  userName: Option[String] = None,
  parallelism: Option[Int] = Some(1)
) extends OutputConf[Row] {
  override def getOutputFormat = JDBCOutput.getOutputFormat(this)

  override def forwardedFieldsIds = rowSchema.forwardedFields
}

/**
  * "Empty" sink (for example, used if one need only to report timings)
  */
case class EmptyOutputConf() extends OutputConf[Row] {
  override def forwardedFieldsIds: Seq[Symbol] = Seq()
  override def getOutputFormat: OutputFormat[Row] = ???
  override def parallelism: Option[Int] = Some(1)
}
