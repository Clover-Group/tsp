package ru.itclover.tsp.io.input

import java.sql.{DriverManager, ResultSetMetaData}
import java.util.Properties
import scala.language.existentials
import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.io.{GenericInputFormat, RichInputFormat}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.core.io.InputSplit
import org.apache.flink.types.Row
import org.apache.flink.api.java.tuple.{Tuple2 => JavaTuple2}
import ru.itclover.tsp.core.Time.TimeExtractor
import ru.itclover.tsp.utils.CollectionsOps.{RightBiasedEither, TryOps}
import ru.itclover.tsp.phases.NumericPhases.SymbolNumberExtractor
import ru.itclover.tsp.utils.UtilityTypes.ThrowableOr
import ru.itclover.tsp.JDBCInputFormatProps
import scala.util.Try

/**
  * Source for anything that support JDBC connection
  * @param sourceId mark to pass to sink
  * @param jdbcUrl example - "jdbc:clickhouse://localhost:8123/default?"
  * @param query SQL query
  * @param driverName example - "ru.yandex.clickhouse.ClickHouseDriver"
  * @param datetimeField
  * @param eventsMaxGapMs maximum gap by which source data will be split, i.e. result incidents will be split by these gaps
  * @param defaultEventsGapMs "typical" gap between events, used to unite nearby incidents in one (sessionization)
  * @param partitionFields fields by which data will be split and paralleled physically
  * @param userName for JDBC auth
  * @param password for JDBC auth
  * @param props extra configs to JDBC `DriverManager.getConnection(`
  * @param parallelism basic parallelism of all computational nodes
  * @param patternsParallelism number of parallel branch nodes after sink stage (node)
  */
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
  props: Option[Map[String, AnyRef]] = None,
  dataTransformation: Option[SourceDataTransformation] = None,
  parallelism: Option[Int] = None,
  numParallelSources: Option[Int] = Some(1),
  patternsParallelism: Option[Int] = Some(2)
) extends InputConf[Row] {

  import InputConf.{getRowFieldOrThrow, getKVFieldOrThrow}
  val properties = new Properties()
  props.getOrElse(Map.empty).foreach(x => properties.put(x._1, x._2))

  lazy val fieldsTypesInfo: ThrowableOr[Seq[(Symbol, TypeInformation[_])]] = {
    val classTry: Try[Class[_]] = Try(Class.forName(driverName))
    properties.put("user", userName.getOrElse(""))
    properties.put("password", password.getOrElse(""))

    val connectionTry = Try(DriverManager.getConnection(jdbcUrl, properties))
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
    setDefaultTimeouts()
    JDBCInputFormatProps
      .buildJDBCInputFormat()
      .addProperties(properties)
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

  // TODO Rm, Temporary timeouts
  def setDefaultTimeouts() = {
    if (properties.getProperty("socket_timeout") == null) properties.setProperty("socket_timeout", "150000")
    if (properties.getProperty("dataTransferTimeout") == null) properties.setProperty("dataTransferTimeout", "100000")
    if (properties.getProperty("keepAliveTimeout") == null) properties.setProperty("keepAliveTimeout", "150000")
  }

  implicit lazy val keyValExtractor: Either[Throwable, Row => (Symbol, AnyRef)] = errOrFieldsIdxMap.map {
    fieldsIdxMap => (event: Row) =>
      val keyAndValueCols = dataTransformation match {
        case Some(ndu @ NarrowDataUnfolding(_, _, _)) => (ndu.key, ndu.value)
        case _ => sys.error("Unsuitable data transformation instance")
      }
      val keyColInd = fieldsIdxMap.getOrElse(keyAndValueCols._1, Int.MaxValue)
      val valueColInd = fieldsIdxMap.getOrElse(keyAndValueCols._2, Int.MaxValue)
      getKVFieldOrThrow(event, keyColInd, valueColInd)
  }
}
