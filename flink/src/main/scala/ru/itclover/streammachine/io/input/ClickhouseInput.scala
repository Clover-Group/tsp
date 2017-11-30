package ru.itclover.streammachine.io.input

import java.sql.{Connection, DriverManager, SQLException}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.api.java.typeutils.{RowTypeInfo, TupleTypeInfoBase}
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import ru.itclover.streammachine.utils.ImplicitUtils.{RightBiasedEither, TryOps}

import scala.util.Try

object ClickhouseInput {

  def getSource(config: JDBCConfig): Either[Throwable, JDBCInputFormat] = {
    for (typesInfoMap <- queryTypeInformation(config)) yield {
      val rowTypesInfo = new RowTypeInfo(typesInfoMap.values.toSeq :_*)
      JDBCInputFormat.buildJDBCInputFormat()
        .setDrivername(config.driverName)
        .setDBUrl(config.jdbcUrl)
        .setUsername(config.userName.getOrElse(""))
        .setPassword(config.password.getOrElse(""))
        .setQuery(config.query)
        .setRowTypeInfo(rowTypesInfo)
        .finish()
    }
  }

  def queryTypeInformation(config: JDBCConfig): Either[Throwable, Map[String, TypeInformation[_]]] = {
    val classTry = Try(Class.forName(config.driverName))
    val connectionTry = Try(DriverManager.getConnection(config.jdbcUrl, config.userName.getOrElse(""),
                                                           config.password.getOrElse("")))
    (for {
      _ <- classTry
      connection <- connectionTry
      resultSet <- Try(connection.createStatement().executeQuery(s"SELECT * FROM (${config.query}) LIMIT 1"))
      metaData <- Try(resultSet.getMetaData)
    } yield {
      ((1 to metaData.getColumnCount) map { i: Int =>
        val className = metaData.getColumnClassName(i)
        (metaData.getColumnName(i), TypeInformation.of(Class.forName(className)))
      }).toMap[String, TypeInformation[_]]
    }).toEither
  }
}
