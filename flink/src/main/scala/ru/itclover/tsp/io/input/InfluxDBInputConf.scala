package ru.itclover.tsp.io.input

import java.time.Instant
import org.apache.flink.api.common.io.RichInputFormat
import scala.util.{Failure, Success, Try}
import collection.JavaConversions._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.types.Row
import org.influxdb.InfluxDBException
import org.influxdb.dto.{Query, QueryResult}
import ru.itclover.tsp.core.Time.TimeExtractor
import ru.itclover.tsp.phases.NumericPhases.SymbolNumberExtractor
import ru.itclover.tsp.services.InfluxDBService
import ru.itclover.tsp.utils.CollectionsOps.{OptionOps, RightBiasedEither, TryOps}
import ru.itclover.tsp.utils.UtilityTypes.ThrowableOr

case class InfluxDBInputConf(
  sourceId: Int,
  dbName: String,
  url: String,
  query: String,
  eventsMaxGapMs: Long,
  defaultEventsGapMs: Long,
  partitionFields: Seq[Symbol],
  datetimeField: Symbol = 'time,
  userName: Option[String] = None,
  password: Option[String] = None,
  timeoutSec: Option[Long] = None,
  parallelism: Option[Int] = None,
  patternsParallelism: Option[Int] = Some(1),
  sinkParallelism: Option[Int] = Some(1)
) extends InputConf[Row] {

  import InfluxDBInputConf._
  import InputConf.getRowFieldOrThrow

  val defaultTimeoutSec = 200L
  lazy val dbConnect =
    InfluxDBService.connectDb(url, dbName, userName, password, timeoutSec.getOrElse(defaultTimeoutSec))

  private val dummyResult: Class[QueryResult.Result] =
    new QueryResult.Result().getClass.asInstanceOf[Class[QueryResult.Result]]
  val resultTypeInfo: TypeInformation[QueryResult.Result] = TypeInformation.of(dummyResult)

  override lazy val fieldsTypesInfo: ThrowableOr[Seq[(Symbol, TypeInformation[_])]] = (for {
    series <- firstSeries
    values <- series.getValues.headOption
      .toTry(whenFail = emptyException)
    tags = if (series.getTags != null) series.getTags.toSeq.sortBy(_._1) else Seq.empty
    // _ <- if (tags.contains(null)) Failure(emptyException) else Success()
  } yield {
    val fields = tags.map(_._1) ++ series.getColumns.toSeq
    val classes = tags.map(_ => classOf[String]) ++ values.map(v => if (v != null) v.getClass else classOf[Double])
    fields.zip(classes).map {
      case (field, clazz) => (Symbol(field), TypeInformation.of(clazz))
    }
  }).toEither
  private val emptyException = new InfluxDBException(s"Empty/Null values or tags in query - `$query`.")

  lazy val errOrFieldsIdxMap = fieldsTypesInfo.map(_.map(_._1).zipWithIndex.toMap)

  implicit lazy val timeExtractor = errOrFieldsIdxMap.map { fieldsIdxMap =>
    val dtField = datetimeField
    new TimeExtractor[Row] {
      override def apply(event: Row) = {
        val isoTime = getRowFieldOrThrow(event, fieldsIdxMap, dtField).asInstanceOf[String]
        Instant.parse(isoTime).toEpochMilli / 1000.0
      }
    }
  }

  implicit lazy val symbolNumberExtractor = errOrFieldsIdxMap.map(
    fieldsIdxMap =>
      new SymbolNumberExtractor[Row] {
        override def extract(event: Row, name: Symbol): Double = {
          getRowFieldOrThrow(event, fieldsIdxMap, name) match {
            case d: java.lang.Double => d
            case f: java.lang.Float  => f.floatValue().toDouble
            case some                => Try(some.toString.toDouble).getOrElse(Double.NaN)
          }
        }
    }
  )

  implicit lazy val anyExtractor =
    errOrFieldsIdxMap.map(fieldsIdxMap => (event: Row, name: Symbol) => getRowFieldOrThrow(event, fieldsIdxMap, name))

  def getInputFormat(fieldTypesInfo: Array[(Symbol, TypeInformation[_])]) = {
    InfluxDBInputFormat
      .create()
      .url(url)
      .timeoutSec(timeoutSec.getOrElse(defaultTimeoutSec))
      .username(userName.getOrElse(""))
      .password(password.getOrElse(""))
      .database(dbName)
      .query(query)
      .and()
      .buildIt()
  }

  lazy val firstSeries = {
    val influxQuery = new Query(InfluxDBService.makeLimit1Query(query), dbName)
    for {
      db     <- dbConnect
      result <- Try(db.query(influxQuery))
      _ <- if (result.hasError) Failure(new InfluxDBException(result.getError))
      else if (result.getResults == null) Failure(new InfluxDBException(s"Null results of query `$influxQuery`."))
      else Success(())
      // Safely get first series
      firstSeries <- result.getResults.headOption
        .flatMap(r => Option(r.getSeries).flatMap(_.headOption))
        .toTry(whenFail = new InfluxDBException(s"Empty results in query - `$query`."))
    } yield firstSeries
  }
}
