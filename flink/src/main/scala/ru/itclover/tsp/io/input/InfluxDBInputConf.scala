/*
package ru.itclover.tsp.io.input

import java.time.Instant
import org.apache.flink.api.common.io.RichInputFormat
import scala.util.{Failure, Success, Try}
import collection.JavaConversions._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.runtime.util.clock.SystemClock
import org.apache.flink.types.Row
import org.influxdb.InfluxDBException
import org.influxdb.dto.{Query, QueryResult}
import cats.syntax.either._
import ru.itclover.tsp.io.input.InputConf.getKVFieldOrThrow
import ru.itclover.tsp.services.InfluxDBService
import ru.itclover.tsp.transformers.SparseRowsDataAccumulator
import ru.itclover.tsp.utils.CollectionsOps.{OptionOps, TryOps}
import ru.itclover.tsp.utils.UtilityTypes.ThrowableOr


/**
  * Source for InfluxDB
  * @param sourceId simple mark to pass to sink
  * @param dbName
  * @param url to database, for example `http://localhost:8086`
  * @param query Influx SQL query for data
  * @param eventsMaxGapMs maximum gap by which source data will be split, i.e. result incidents will be split by these gaps
  * @param defaultEventsGapMs "typical" gap between events, used to unite nearby incidents in one (sessionization)
  * @param partitionFields fields by which data will be split and paralleled physically
  * @param datetimeField name of datetime field, could be timestamp and regular time (will be parsed by JodaTime)
  * @param userName for auth
  * @param password for auth
  * @param timeoutSec for DB connection
  * @param parallelism of source task (not recommended to chagne)
  * @param numParallelSources number of absolutely separate sources to create. Patterns also will be separated by
  *                           equal (as much as possible) buckets by the max window in pattern (TBD by sum window size)
  * @param patternsParallelism number of parallel branch nodes splitted after sink stage (node). Patterns also
  *                            separated by approx. equal buckets by the max window in pattern (TBD by sum window size)
  */
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
  dataTransformation: Option[SourceDataTransformation] = None,
  parallelism: Option[Int] = None,
  numParallelSources: Option[Int] = Some(1),
  patternsParallelism: Option[Int] = Some(2)
) extends InputConf[Row] {

  import InfluxDBInputConf._
  import InputConf.getRowFieldOrThrow

  val defaultTimeoutSec = 200L
  def dbConnect =
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

  lazy val errOrTransformedFieldsIdxMap = dataTransformation match {
    case Some(NarrowDataUnfolding(_, _, _, _)) | Some(WideDataFilling(_, _)) =>
      try {
        Right(SparseRowsDataAccumulator.fieldsIndexesMap(this))
      } catch {
        case t: Throwable => Left(t)
      }
    case _ => errOrFieldsIdxMap
  }

  def firstSeries = {
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

*/
