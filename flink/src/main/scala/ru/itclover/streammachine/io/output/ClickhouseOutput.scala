package ru.itclover.streammachine.io.output

// import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink
import com.typesafe.scalalogging.Logger
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat

object ClickhouseOutput {
  val log = Logger("ClickhouseOutput")
  val DEFAULT_BATCH_INTERVAL = 1000000

  def getOutputFormat(config: JDBCOutputConf): JDBCOutputFormat = {
    val insertQuery = getInsertQuery(config.sinkSchema)
    log.info(s"Configure ClickhouseOutput with insertQuery = `$insertQuery`")
    JDBCOutputFormat.buildJDBCOutputFormat()
        .setDrivername(config.driverName)
        .setDBUrl(config.jdbcUrl)
        .setUsername(config.userName.getOrElse(""))
        .setPassword(config.password.getOrElse(""))
        .setQuery(insertQuery)
        .setSqlTypes(config.sinkSchema.fieldTypes.toArray)
        .setBatchInterval(config.batchInterval.getOrElse(DEFAULT_BATCH_INTERVAL))
        .finish()
  }

  private def getInsertQuery(sinkSchema: JDBCSegmentsSink) = {
    val columns = sinkSchema.fieldsNames.map(_.toString().tail)
    val statements = columns.map(_ => "?").mkString(", ")
    s"INSERT INTO ${sinkSchema.tableName} (${columns.mkString(", ")}) VALUES (${statements})"
  }
}
