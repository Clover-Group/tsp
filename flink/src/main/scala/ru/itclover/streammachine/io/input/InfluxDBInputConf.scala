package ru.itclover.streammachine.io.input

import java.time.Instant
import org.apache.flink.api.common.io.RichInputFormat
import scala.util.{Failure, Success, Try}
import collection.JavaConversions._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.types.Row
import org.influxdb.InfluxDBException
import org.influxdb.dto.{Query, QueryResult}
import ru.itclover.streammachine.core.Time.TimeExtractor
import ru.itclover.streammachine.phases.NumericPhases.SymbolNumberExtractor
import ru.itclover.streammachine.services.InfluxDBService
import ru.itclover.streammachine.utils.CollectionsOps.{OptionOps, RightBiasedEither, TryOps}
import ru.itclover.streammachine.utils.UtilityTypes.ThrowableOr


case class InfluxDBInputConf(sourceId: Int,
                             dbName: String,
                             url: String,
                             query: String,
                             eventsMaxGapMs: Long,
                             partitionFields: Seq[Symbol],
                             datetimeField: Symbol = 'time,
                             userName: Option[String] = None,
                             password: Option[String] = None,
                             parallelism: Option[Int] = None,
                             timeoutSec: Option[Long] = None) extends InputConf[Row] {
  val defaultTimeoutSec = 200L

  lazy val dbConnect = InfluxDBService.connectDb(url, dbName, userName, password,
    timeoutSec.getOrElse(defaultTimeoutSec))

  private val dummyResult: Class[QueryResult.Result] = new QueryResult.Result().getClass.asInstanceOf[Class[QueryResult.Result]]
  val resultTypeInfo: TypeInformation[QueryResult.Result] = TypeInformation.of(dummyResult)


  override lazy val fieldsTypesInfo: ThrowableOr[Seq[(Symbol, TypeInformation[_])]] = (for {
    series <- firstSeries
    values <- series.getValues.headOption
      .toTry(whenFail=emptyException)
      /*.flatMap(v => if(v.contains(null)) Failure(emptyException)
                    else Success(v))*/
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
        case d: java.lang.Double => d
        case f: java.lang.Float => f.floatValue().toDouble
        case some => Try(some.toString.toDouble).getOrElse(Double.NaN)
      }
    }
  )

  implicit lazy val anyExtractor = errOrFieldsIdxMap.map(fieldsIdxMap =>
    (event: Row, name: Symbol) => event.getField(fieldsIdxMap(name))
  )

  def getInputFormat(fieldTypesInfo: Array[(Symbol, TypeInformation[_])]) = {
    InfluxDBInputFormat.create()
      .url(url)
      .timeoutSec(timeoutSec.getOrElse(defaultTimeoutSec))
      .username(userName.getOrElse(""))
      .password(password.getOrElse(""))
      .database(dbName)
      .query(query)
      .and().buildIt()
  }

  lazy val firstSeries = {
    val influxQuery = new Query(InfluxDBService.makeLimit1Query(query), dbName)
    for {
      db <- dbConnect
      result <- Try(db.query(influxQuery))
      _      <- if (result.hasError) Failure(new InfluxDBException(result.getError)) else
                  if (result.getResults == null) Failure(new InfluxDBException(s"Null results of query `$influxQuery`."))
                  else Success(())
      // Safely get first series
      firstSeries <- result.getResults.headOption.flatMap(r => Option(r.getSeries).flatMap(_.headOption))
        .toTry(whenFail=new InfluxDBException(s"Empty results in query - `$query`."))
    } yield firstSeries
  }
}
