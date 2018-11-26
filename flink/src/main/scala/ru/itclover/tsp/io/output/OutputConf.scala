package ru.itclover.tsp.io.output

import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.types.Row

trait OutputConf[Event, EKey] {
  def forwardedFields: Seq[EKey]

  def getOutputFormat: OutputFormat[Event]

  def parallelism: Option[Int]
}

/**
  * Sink for anything that support JDBC connection
  * @param tableName
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
) extends OutputConf[Row, Symbol] {
  override def getOutputFormat = JDBCOutput.getOutputFormat(this)

  override def forwardedFields = rowSchema.forwardedFields
}
