package ru.itclover.tsp.services

import java.util.concurrent.TimeUnit
import java.util.regex.Pattern

import okhttp3.OkHttpClient
import org.influxdb.{InfluxDB, InfluxDBException, InfluxDBFactory}
import org.influxdb.dto.Query
import org.influxdb.{InfluxDB, InfluxDBException, InfluxDBFactory}
import ru.itclover.tsp.utils.CollectionsOps.{OptionOps, StringOps}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

object InfluxDBService {
  case class InfluxConf(
    url: String,
    dbName: String,
    userName: Option[String] = None,
    password: Option[String] = None,
    timeoutSec: Long
  )

  def fetchFieldsTypesInfo(query: String, conf: InfluxConf): Try[Seq[(Symbol, Class[_])]] = for {
    db     <- connectDb(conf)
    series <- fetchFirstSeries(db, query, conf.dbName)
    seriesForTypes <- fetchFirstNotNullValueSeries(db, query, conf.dbName)
    values <- seriesForTypes.getValues.asScala.headOption.toTry(whenNone = emptyValuesException(query))
    tags = if (series.getTags != null) series.getTags.asScala.toSeq.sortBy(_._1) else Seq.empty
  } yield {
    val fields = tags.map(_._1) ++ series.getColumns.asScala
    val classes = tags.map(_ => classOf[String]) ++ values.asScala.map(
        v => if (v != null) v.getClass else classOf[Nothing]
      )
    val res = fields.map(Symbol(_)).zip(classes)
    res
  }

  def fetchFirstSeries(db: InfluxDB, query: String, dbName: String) = {
    val influxQuery = new Query(makeLimit1Query(query), dbName)
    for {
      result <- Try(db.query(influxQuery))
      _ <- if (result.hasError) Failure(new InfluxDBException(result.getError))
      else if (result.getResults == null) Failure(new InfluxDBException(s"Null results of query `$influxQuery`."))
      else Success(())
      // Safely get first series
      firstSeries <- result.getResults.asScala.headOption
        .flatMap(r => Option(r.getSeries.asScala).flatMap(_.headOption))
        .toTry(whenNone = new InfluxDBException(s"Empty results in query - `$query`."))
    } yield firstSeries
  }

  def fetchFirstNotNullValueSeries(db: InfluxDB, query: String, dbName: String) = {
    val influxQuery = new Query(makeFirstNotNullQuery(query), dbName)
    for {
      result <- Try(db.query(influxQuery))
      _ <- if (result.hasError) Failure(new InfluxDBException(result.getError))
      else if (result.getResults == null) Failure(new InfluxDBException(s"Null results of query `$influxQuery`."))
      else Success(())
      // Safely get first series
      firstSeries <- result.getResults.asScala.headOption
        .flatMap(r => Option(r.getSeries.asScala).flatMap(_.headOption))
        .toTry(whenNone = new InfluxDBException(s"Empty results in query - `$query`."))
    } yield firstSeries
  }

  def connectDb(conf: InfluxConf) = {
    import conf._
    val extraConf = new OkHttpClient.Builder()
      .readTimeout(timeoutSec, TimeUnit.SECONDS)
      .writeTimeout(timeoutSec, TimeUnit.SECONDS)
      .connectTimeout(timeoutSec, TimeUnit.SECONDS)
    for {
      connection <- Try(InfluxDBFactory.connect(url, userName.orNull, password.orNull, extraConf))
      db         <- Try(connection.setDatabase(dbName))
    } yield db
  }

  def makeLimit1Query(query: String) =
    query.replaceLast("""LIMIT \d+""", "", Pattern.CASE_INSENSITIVE) + " LIMIT 1"

  def makeFirstNotNullQuery(query: String) = s"SELECT first(*) from ($query)"

  def emptyValuesException(query: String) = new InfluxDBException(s"Empty/Null values or tags in query - `$query`.")
}
