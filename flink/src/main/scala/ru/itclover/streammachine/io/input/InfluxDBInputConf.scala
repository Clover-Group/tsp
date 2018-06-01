package ru.itclover.streammachine.io.input

import java.time.Instant

import scala.util.{Failure, Success, Try}
import collection.JavaConversions._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.InfluxDBInputFormat
import org.apache.flink.types.Row
import org.influxdb.InfluxDBException
import org.influxdb.dto.{Query, QueryResult}
import org.influxdb.impl.TimeUtil
import ru.itclover.streammachine.core.Time.TimeExtractor
import ru.itclover.streammachine.phases.NumericPhases.SymbolNumberExtractor
import ru.itclover.streammachine.services.InfluxDBService
import ru.itclover.streammachine.http.utils.ImplicitUtils.{OptionOps, RightBiasedEither, TryOps}


case class InfluxDBInputConf(sourceId: Int,
                             dbName: String, url: String,
                             query: String,
                             datetimeField: Symbol,
                             eventsMaxGapMs: Long,
                             partitionFields: Seq[Symbol],
                             userName: Option[String] = None,
                             password: Option[String] = None) extends InputConf[Row] {

  lazy val connectionAndDb = InfluxDBService.connectDb(url, dbName, userName, password)

  private val dummyResult: Class[QueryResult.Result] = new QueryResult.Result().getClass.asInstanceOf[Class[QueryResult.Result]]
  val resultTypeInfo: TypeInformation[QueryResult.Result] = TypeInformation.of(dummyResult)


  override lazy val fieldsTypesInfo: InputConf.ThrowableOrTypesInfo = (for {
    (_, db) <- connectionAndDb
    series <- firstSeries
    values <- series.getValues.headOption.toTry(new InfluxDBException(s"Empty values in query - $query."))
  } yield {
    val tags = if (series.getTags != null) series.getTags.toSeq.sortBy(_._1) else Seq.empty
    val classes = tags.map(_.getClass) ++ values.map(_.getClass)
    series.getColumns.zip(classes).map {
      case (field, clazz) => (Symbol(field), TypeInformation.of(clazz))
    }
  }).toEither


  private lazy val errOrFieldsIdxMap = fieldsTypesInfo.map(_.map(_._1).zipWithIndex.toMap)


  implicit lazy val timeExtractor = {
    val timeIndOrErr = errOrFieldsIdxMap.map(_.apply(datetimeField))
    timeIndOrErr.map(timeInd => new TimeExtractor[Row] {
      override def apply(event: Row) = {
        val isoTime = event.getField(timeInd).asInstanceOf[String]
        Instant.parse(isoTime).toEpochMilli / 1000.0
      }
    })
  }

  implicit lazy val symbolNumberExtractor = errOrFieldsIdxMap.map(fieldsIdxMap =>
    new SymbolNumberExtractor[Row] {
      override def extract(event: Row, symbol: Symbol): Double = event.getField(fieldsIdxMap(symbol)) match {
        case d: java.lang.Double => d.doubleValue()
        case f: java.lang.Float => f.floatValue().toDouble
        case err => throw new ClassCastException(s"Cannot cast value $err to float or double.")
      }
    }
  )

  implicit lazy val anyExtractor = errOrFieldsIdxMap.map(fieldsIdxMap =>
    (event: Row, name: Symbol) => event.getField(fieldsIdxMap(name))
  )

  def getInputFormat = InfluxDBInputFormat.create()
    .url(url)
    .username(userName.getOrElse(""))
    .password(password.getOrElse(""))
    .database(dbName)
    .query(query)
    .and().buildIt()

  lazy val firstSeries = {
    val influxQuery = new Query(InfluxDBService.makeLimit1Query(query), dbName)
    for {
      (_, db) <- connectionAndDb
      result <- Try(db.query(influxQuery))
      _      <- if (result.hasError) Failure(new InfluxDBException(result.getError)) else Success(())

      firstSeries <- result.getResults.headOption.flatMap(_.getSeries.headOption)
        .toTry(new InfluxDBException(s"Empty results in query - $query."))
    } yield firstSeries
  }
}
