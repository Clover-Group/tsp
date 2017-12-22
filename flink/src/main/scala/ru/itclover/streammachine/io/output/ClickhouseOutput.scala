package ru.itclover.streammachine.io.output

// import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat

object ClickhouseOutput {
  val DEFAULT_BATCH_INTERVAL = 1000000

  def getOutputFormat(config: JDBCConfig): JDBCOutputFormat = {
    val insertQuery = getInsertQuery(config.sinkTable, config.sinkColumnsNames.map(_.toString().tail))
    println(s"Configure ClickhouseOutput with insertQuery = `$insertQuery`")
    JDBCOutputFormat.buildJDBCOutputFormat()
        .setDrivername(config.driverName)
        .setDBUrl(config.jdbcUrl)
        .setUsername(config.userName.getOrElse(""))
        .setPassword(config.password.getOrElse(""))
        .setQuery(insertQuery)
        .setBatchInterval(config.batchInterval.getOrElse(DEFAULT_BATCH_INTERVAL))
        .finish()
  }

  private def getInsertQuery(tableName: String, columnsNames: List[String]) =
    s"INSERT INTO $tableName (${columnsNames.mkString(", ")}) VALUES (${columnsNames.map(_ => "?").mkString(", ")})"
}
