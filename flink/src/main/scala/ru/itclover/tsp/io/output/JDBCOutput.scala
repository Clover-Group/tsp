package ru.itclover.tsp.io.output

// import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink
import com.typesafe.scalalogging.Logger
import org.apache.flink.connector.jdbc.JdbcOutputFormat

object JDBCOutput {
  val log = Logger("ClickhouseOutput")
  val DEFAULT_BATCH_INTERVAL = 1000000

  def getOutputFormat(config: JDBCOutputConf): JdbcOutputFormat = {
    val insertQuery = getInsertQuery(config.tableName, config.rowSchema)
    log.info(s"Configure ClickhouseOutput with insertQuery = `$insertQuery`")
    JdbcOutputFormat
      .buildJdbcOutputFormat()
      .setDrivername(config.driverName)
      .setDBUrl(config.jdbcUrl)
      .setUsername(config.userName.getOrElse(""))
      .setPassword(config.password.getOrElse(""))
      .setQuery(insertQuery)
      .setSqlTypes(config.rowSchema.fieldsTypes.toArray)
      .setBatchSize(config.batchInterval.getOrElse(DEFAULT_BATCH_INTERVAL))
      .finish()
  }

  private def getInsertQuery(tableName: String, rowSchema: EventSchema) = {
    val columns = rowSchema.fieldsNames.map(_.toString().tail)
    val statements = columns.map(_ => "?").mkString(", ")
    s"INSERT INTO ${tableName} (${columns.mkString(", ")}) VALUES (${statements})"
  }
}
