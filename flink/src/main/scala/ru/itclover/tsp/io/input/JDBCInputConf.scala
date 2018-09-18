package ru.itclover.tsp.io.input

import java.sql.DriverManager
import org.apache.flink.api.common.io.{GenericInputFormat, RichInputFormat}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.core.io.InputSplit
import org.apache.flink.types.Row
import ru.itclover.tsp.core.Time.TimeExtractor
import ru.itclover.tsp.utils.CollectionsOps.{RightBiasedEither, TryOps}
import ru.itclover.tsp.phases.NumericPhases.SymbolNumberExtractor
import ru.itclover.tsp.utils.UtilityTypes.ThrowableOr
import scala.util.Try

case class JDBCInputConf(
  sourceId: Int,
  jdbcUrl: String,
  query: String,
  driverName: String,
  datetimeField: Symbol,
  eventsMaxGapMs: Long,
  defaultEventsGapMs: Long,
  partitionFields: Seq[Symbol],
  userName: Option[String] = None,
  password: Option[String] = None,
  parallelism: Option[Int] = None
) extends InputConf[Row] {

  import InputConf.getRowFieldOrThrow

  lazy val fieldsTypesInfo: ThrowableOr[Seq[(Symbol, TypeInformation[_])]] = {
    val classTry = Try(Class.forName(driverName))
    val connectionTry = Try(DriverManager.getConnection(jdbcUrl, userName.getOrElse(""), password.getOrElse("")))
    (for {
      _          <- classTry
      connection <- connectionTry
      resultSet  <- Try(connection.createStatement().executeQuery(s"SELECT * FROM (${query}) as mainQ LIMIT 1"))
      metaData   <- Try(resultSet.getMetaData)
    } yield {
      (1 to metaData.getColumnCount) map { i: Int =>
        val className = metaData.getColumnClassName(i)
        (metaData.getColumnName(i), TypeInformation.of(Class.forName(className)))
      }
    }).toEither map (_ map { case (name, ti) => Symbol(name) -> ti })
  }

  def getInputFormat(fieldTypesInfo: Array[(Symbol, TypeInformation[_])]): RichInputFormat[Row, InputSplit] = {
    val rowTypesInfo = new RowTypeInfo(fieldTypesInfo.map(_._2), fieldTypesInfo.map(_._1.toString.tail))
    JDBCInputFormat
      .buildJDBCInputFormat()
      .setDrivername(driverName)
      .setDBUrl(jdbcUrl)
      .setUsername(userName.getOrElse(""))
      .setPassword(password.getOrElse(""))
      .setQuery(query)
      .setRowTypeInfo(rowTypesInfo)
      .finish()
  }

  lazy val errOrFieldsIdxMap = fieldsTypesInfo.map(_.map(_._1).zipWithIndex.toMap)

  implicit lazy val timeExtractor = errOrFieldsIdxMap map { fieldsIdxMap =>
    new TimeExtractor[Row] {
      override def apply(event: Row) =
        getRowFieldOrThrow(event, fieldsIdxMap, datetimeField).asInstanceOf[Double]
    }
  }

  implicit lazy val symbolNumberExtractor = errOrFieldsIdxMap.map(
    fieldsIdxMap =>
      new SymbolNumberExtractor[Row] {
        override def extract(event: Row, name: Symbol): Double =
          getRowFieldOrThrow(event, fieldsIdxMap, name) match {
            case d: java.lang.Double => d
            case f: java.lang.Float  => f.doubleValue()
            case some =>
              Try(some.toString.toDouble).getOrElse(throw new ClassCastException(s"Cannot cast value $some to double."))
          }
    }
  )

  implicit lazy val anyExtractor = errOrFieldsIdxMap.map { fieldsIdxMap => (event: Row, name: Symbol) =>
    getRowFieldOrThrow(event, fieldsIdxMap, name)
  }
}
