package ru.itclover.streammachine.io.input

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.api.java.typeutils.{RowTypeInfo, TupleTypeInfoBase}
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo

object ClickhouseInput {

  def getSource(config: JDBCConfig, rowTypeInfo: RowTypeInfo): JDBCInputFormat = {

    JDBCInputFormat.buildJDBCInputFormat()
      .setDrivername(config.driverName)
      .setDBUrl(config.jdbcUrl)
      .setUsername(config.userName.getOrElse(""))
      .setPassword(config.password.getOrElse(""))
      .setQuery(config.query)
      .setRowTypeInfo(rowTypeInfo)
      .finish()
  }

}
