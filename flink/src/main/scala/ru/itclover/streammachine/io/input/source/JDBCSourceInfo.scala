package ru.itclover.streammachine.io.input.source

import java.sql.DriverManager
import com.typesafe.scalalogging.Logger
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.types.Row
import ru.itclover.streammachine.io.input.JDBCInputConf
import ru.itclover.streammachine.http.utils.ImplicitUtils.RightBiasedEither
import ru.itclover.streammachine.http.utils.ImplicitUtils.TryOps
import scala.util.Try


case class JDBCSourceInfo(config: JDBCInputConf,
                          fieldsInfo: Seq[(Symbol, TypeInformation[_])],
                          inputFormat: JDBCInputFormat)
  extends SourceInfo[Row] with Serializable {
  require(config.partitionColnames.nonEmpty)

  val fieldsNames: Seq[Symbol] = fieldsInfo.map(_._1)

  val datetimeFieldName: Symbol = config.datetimeColname

  val fieldsIndexesMap: Map[Symbol, Int] = fieldsNames.zipWithIndex.toMap

  val partitionIndex: Int = fieldsIndexesMap(config.partitionColnames.head)
}


object JDBCSourceInfo {
  private val log = Logger[JDBCSourceInfo]

  def apply(config: JDBCInputConf): Either[Throwable, JDBCSourceInfo] = for {
    fieldsInfo <- queryFieldsTypeInformation(config)
  } yield {
    log.info(s"Successfully queried fields types info: `${fieldsInfo.mkString(", ")}")
    val format = getInputFormat(config, fieldsInfo.toArray)
    JDBCSourceInfo(config, fieldsInfo.map { case (name, ti) => Symbol(name) -> ti }, format)
  }


  private def getInputFormat(config: JDBCInputConf, fieldTypesInfo: Array[(String, TypeInformation[_])]): JDBCInputFormat = {
    val rowTypesInfo = new RowTypeInfo(fieldTypesInfo.map(_._2), fieldTypesInfo.map(_._1))
    JDBCInputFormat.buildJDBCInputFormat()
      .setDrivername(config.driverName)
      .setDBUrl(config.jdbcUrl)
      .setUsername(config.userName.getOrElse(""))
      .setPassword(config.password.getOrElse(""))
      .setQuery(config.query)
      .setRowTypeInfo(rowTypesInfo)
      .finish()
  }

  private def queryFieldsTypeInformation(config: JDBCInputConf): Either[Throwable, IndexedSeq[(String, TypeInformation[_])]] = {
    val classTry = Try(Class.forName(config.driverName))
    val connectionTry = Try(DriverManager.getConnection(config.jdbcUrl, config.userName.getOrElse(""),
                                                        config.password.getOrElse("")))
    (for {
      _ <- classTry
      connection <- connectionTry
      resultSet <- Try(connection.createStatement().executeQuery(s"SELECT * FROM (${config.query}) LIMIT 1"))
      metaData <- Try(resultSet.getMetaData)
    } yield {
      (1 to metaData.getColumnCount) map { i: Int =>
        val className = metaData.getColumnClassName(i)
        (metaData.getColumnName(i), TypeInformation.of(Class.forName(className)))
      }
    }).toEither
  }
}
