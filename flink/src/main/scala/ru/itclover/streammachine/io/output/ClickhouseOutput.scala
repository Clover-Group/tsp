package ru.itclover.streammachine.io.output

// import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat
import ru.itclover.streammachine.io.output.JDBCConfig

object ClickhouseOutput {
  val DEFAULT_BATCH_INTERVAL = 5000

  // TODO Naming
  def getOutputFormat(config: JDBCConfig): Either[Throwable, JDBCOutputFormat] = {
    val insertQuery = getInsertQuery(config.sinkTable, config.sinkColumnsNames)
    val jdbcOutput = JDBCOutputFormat.buildJDBCOutputFormat()
        .setDrivername(config.driverName)
        .setDBUrl(config.jdbcUrl)
        .setUsername(config.userName.getOrElse(""))
        .setPassword(config.password.getOrElse(""))
        .setQuery(insertQuery)
        .setBatchInterval(config.batchInterval.getOrElse(DEFAULT_BATCH_INTERVAL))
        .finish()

    Right(jdbcOutput)
  }


  private def getInsertQuery(tableName: String, columnsNames: List[Symbol]) =
    s"INSERT INTO $tableName (${columnsNames.mkString(", ")}) VALUES (${columnsNames.map(_ => '?).mkString(", ")})"
}
