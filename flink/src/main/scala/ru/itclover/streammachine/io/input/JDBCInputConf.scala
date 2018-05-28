package ru.itclover.streammachine.io.input

import java.sql.DriverManager

import com.typesafe.scalalogging.Logger
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo
import ru.itclover.streammachine.http.utils.ImplicitUtils.RightBiasedEither
import ru.itclover.streammachine.http.utils.ImplicitUtils.TryOps
import ru.itclover.streammachine.io.input.InputConf.ThrowableOrTypesInfo
import scala.util.Try


case class JDBCInputConf(sourceId: Int,
                         jdbcUrl: String,
                         query: String,
                         driverName: String,
                         datetimeFieldName: Symbol,
                         eventsMaxGapMs: Long,
                         partitionFieldNames: Seq[Symbol],
                         userName: Option[String] = None,
                         password: Option[String] = None
                        ) extends InputConf {

  lazy val fieldsTypesInfo: ThrowableOrTypesInfo = {
    val classTry = Try(Class.forName(driverName))
    val connectionTry = Try(DriverManager.getConnection(jdbcUrl, userName.getOrElse(""), password.getOrElse("")))
    (for {
      _ <- classTry
      connection <- connectionTry
      resultSet <- Try(connection.createStatement().executeQuery(s"SELECT * FROM (${query}) as mainQ LIMIT 1"))
      metaData <- Try(resultSet.getMetaData)
    } yield {
      (1 to metaData.getColumnCount) map { i: Int =>
        val className = metaData.getColumnClassName(i)
        (metaData.getColumnName(i), TypeInformation.of(Class.forName(className)))
      }
    }).toEither map (_ map { case (name, ti) => Symbol(name) -> ti })
  }

  def getInputFormat(fieldTypesInfo: Array[(Symbol, TypeInformation[_])]): JDBCInputFormat = {
    val rowTypesInfo = new RowTypeInfo(fieldTypesInfo.map(_._2), fieldTypesInfo.map(_._1.toString.tail))
    JDBCInputFormat.buildJDBCInputFormat()
      .setDrivername(driverName)
      .setDBUrl(jdbcUrl)
      .setUsername(userName.getOrElse(""))
      .setPassword(password.getOrElse(""))
      .setQuery(query)
      .setRowTypeInfo(rowTypesInfo)
      .finish()
  }
}
