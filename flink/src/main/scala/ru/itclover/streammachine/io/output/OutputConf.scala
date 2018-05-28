package ru.itclover.streammachine.io.output

trait OutputConf


case class JDBCOutputConf(tableName: String,
                          rowSchema: RowSchema,
                          jdbcUrl: String,
                          driverName: String,
                          password: Option[String] = None,
                          batchInterval: Option[Int] = None,
                          userName: Option[String] = None) extends OutputConf