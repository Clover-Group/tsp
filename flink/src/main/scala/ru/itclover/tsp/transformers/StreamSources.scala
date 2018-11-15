package ru.itclover.tsp.transformers

import java.sql.DriverManager
import java.util
import org.apache.flink.api.common.io.RichInputFormat
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.core.io.InputSplit
import collection.JavaConversions._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import ru.itclover.tsp.io.input.{InfluxDBInputConf, InputConf, JDBCInputConf, NarrowDataUnfolding}
import ru.itclover.tsp.utils.CollectionsOps.{RightBiasedEither, TryOps}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.types.Row
import org.influxdb.dto.QueryResult
import ru.itclover.tsp.utils.UtilityTypes.ThrowableOr
import ru.itclover.tsp.JDBCInputFormatProps
import ru.itclover.tsp.core.Time.{TimeExtractor, TimeNonTransformedExtractor}
import ru.itclover.tsp.io.input.InputConf.{getKVFieldOrThrow, getRowFieldOrThrow}
import ru.itclover.tsp.phases.NumericPhases.{IndexNumberExtractor, SymbolNumberExtractor}
import ru.itclover.tsp.phases.Phases.{AnyExtractor, AnyNonTransformedExtractor}
import scala.collection.mutable
import scala.util.Try

trait StreamSource[Event] {
  def createStream: Either[Throwable, DataStream[Event]]

  def conf: InputConf[Event]

  def emptyEvent: Event

  def getTerminalCheck: Either[Throwable, Event => Boolean]

  protected def findNullField(allFields: Seq[Symbol], excludedFields: Seq[Symbol]) = {
    allFields.find { field =>
      !excludedFields.contains(field)
    } match {
      case Some(nullField) => Right(nullField)
      case None =>
        Left(new IllegalArgumentException(s"Fail to compute nullIndex, query contains only date and partition cols."))
    }
  }
}

trait StreamSourceCreator {
  def create[Event](inputConf: InputConf[Event])
}

object JdbcSource {
  def fromFieldsMap(fieldsIndexesMap: Map[Symbol, Int], inputConf: JDBCInputConf) = {
    
  }
}

case class JdbcSource(conf: JDBCInputConf)(implicit streamEnv: StreamExecutionEnvironment)
    extends StreamSource[Row] with FromRowExtractors {
  val stageName = "JDBC input processing stage"

  override def createStream = for {
    fTypesInfo <- fieldsTypesInfo
  } yield {
    val stream = streamEnv
      .createInput(getInputFormat(fTypesInfo.toArray))
      .name(stageName)
    conf.parallelism match {
      case Some(p) => stream.setParallelism(p)
      case None    => stream
    }
  }
  
  def getInputFormat(fieldTypesInfo: Array[(Symbol, TypeInformation[_])]): RichInputFormat[Row, InputSplit] = {
    val rowTypesInfo = new RowTypeInfo(fieldTypesInfo.map(_._2), fieldTypesInfo.map(_._1.toString.tail))
    JDBCInputFormatProps
      .buildJDBCInputFormat()
      .setDrivername(conf.driverName)
      .setDBUrl(conf.jdbcUrl)
      .setUsername(conf.userName.getOrElse(""))
      .setPassword(conf.password.getOrElse(""))
      .setQuery(conf.query)
      .setRowTypeInfo(rowTypesInfo)
      .finish()
  }

  lazy val fieldsTypesInfo: ThrowableOr[Seq[(Symbol, TypeInformation[_])]] = {
    val classTry: Try[Class[_]] = Try(Class.forName(conf.driverName))

    val connectionTry = Try(DriverManager.getConnection(conf.jdbcUrl))
    (for {
      _          <- classTry
      connection <- connectionTry
      resultSet  <- Try(connection.createStatement().executeQuery(s"SELECT * FROM (${conf.query}) as mainQ LIMIT 1"))
      metaData   <- Try(resultSet.getMetaData)
    } yield {
      (1 to metaData.getColumnCount) map { i: Int =>
        val className = metaData.getColumnClassName(i)
        (metaData.getColumnName(i), TypeInformation.of(Class.forName(className)))
      }
    }).toEither map (_ map { case (name, ti) => Symbol(name) -> ti })
  }

  override def emptyEvent = nullEvent match {
    case Right(e) => e
    case Left(ex) => throw ex
  }

  def nullEvent = for {
    fieldsIdxMap <- conf.errOrFieldsIdxMap
  } yield {
    val r = new Row(fieldsIdxMap.size)
    fieldsIdxMap.foreach { case (_, ind) => r.setField(ind, 0) }
    r
  }

  override def getTerminalCheck = for {
    fieldsIdxMap <- conf.errOrFieldsIdxMap
    nullField    <- findNullField(fieldsIdxMap.keys.toSeq, conf.datetimeField +: conf.partitionFields)
    nullInd      = fieldsIdxMap(nullField)
  } yield (event: Row) => event.getArity > nullInd && event.getField(nullInd) == null
}


trait FromRowExtractors {
  
  def fieldsIdxMap: Map[Symbol, Int]
  def datetimeField: Symbol

  implicit lazy val timeExtractor = new TimeExtractor[Row] {
    override def apply(event: Row) =
      getRowFieldOrThrow(event, fieldsIdxMap, datetimeField).asInstanceOf[Double]
  }

  implicit lazy val symbolNumberExtractor = new SymbolNumberExtractor[Row] {
    override def extract(event: Row, name: Symbol): Double =
      getRowFieldOrThrow(event, fieldsIdxMap, name) match {
        case d: java.lang.Double => d
        case f: java.lang.Float  => f.doubleValue()
        case some =>
          Try(some.toString.toDouble).getOrElse(throw new ClassCastException(s"Cannot cast value $some to double."))
      }
  }

  implicit lazy val indexNumberExtractor = new IndexNumberExtractor[Row] {
    override def extract(event: Row, index: Int): Double =
      getRowFieldOrThrow(event, index) match {
        case d: java.lang.Double => d
        case f: java.lang.Float  => f.doubleValue()
        case some =>
          Try(some.toString.toDouble).getOrElse(throw new ClassCastException(s"Cannot cast value $some to double."))
      }
  }

  implicit lazy val anyExtractor = new AnyExtractor[Row] {
    def apply(event: Row, name: Symbol): AnyRef = getRowFieldOrThrow(event, fieldsIdxMap, name)
  }
}




case class InfluxDBSource(conf: InfluxDBInputConf)(implicit streamEnv: StreamExecutionEnvironment)
    extends StreamSource[Row] {
  val dummyResult: Class[QueryResult.Result] = new QueryResult.Result().getClass.asInstanceOf[Class[QueryResult.Result]]
  val queryResultTypeInfo: TypeInformation[QueryResult.Result] = TypeInformation.of(dummyResult)
  val stageName = "InfluxDB input processing stage"

  override def createStream = for {
    fieldsTypesInfo <- conf.fieldsTypesInfo
    fieldsIdxMap    <- conf.errOrFieldsIdxMap
  } yield {
    streamEnv
      .createInput(conf.getInputFormat(fieldsTypesInfo.toArray))(queryResultTypeInfo)
      .flatMap(queryResult => {
        // extract Flink.rows form series of points
        if (queryResult == null || queryResult.getSeries == null) {
          mutable.Buffer[Row]()
        } else
          for {
            series   <- queryResult.getSeries
            valueSet <- series.getValues
          } yield {
            val tags = if (series.getTags != null) series.getTags else new util.HashMap[String, String]()
            val row = new Row(tags.size() + valueSet.size())
            val fieldsAndValues = tags ++ series.getColumns.toSeq.zip(valueSet)
            fieldsAndValues.foreach {
              case (field, value) => row.setField(fieldsIdxMap(Symbol(field)), value)
            }
            row
          }
      })
      .name(stageName)
  }

  override def emptyEvent = nullEvent match {
    case Right(e) => e
    case Left(ex) => throw ex
  }

  def nullEvent = for {
    fieldsIdxMap <- conf.errOrFieldsIdxMap
  } yield {
    val r = new Row(fieldsIdxMap.size)
    fieldsIdxMap.foreach { case (_, ind) => r.setField(ind, 0) }
    r
  }

  override def getTerminalCheck = for {
    fieldsIdxMap <- conf.errOrFieldsIdxMap
    nullField    <- findNullField(fieldsIdxMap.keys.toSeq, conf.datetimeField +: conf.partitionFields)
    nullInd = fieldsIdxMap(nullField)
  } yield (event: Row) => event.getArity > nullInd && event.getField(nullInd) == null
}
