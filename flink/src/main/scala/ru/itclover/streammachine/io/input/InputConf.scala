package ru.itclover.streammachine.io.input

import java.util

import cats.syntax.monad.catsSyntaxMonadIdOps
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.InfluxDBInputFormat
import org.apache.flink.types.Row
import org.influxdb.{BatchOptions, InfluxDB, InfluxDBFactory}
import org.influxdb.{InfluxDBException, InfluxDBIOException}
import org.influxdb.dto.{Query, QueryResult}
import ru.itclover.streammachine.core.Time.TimeExtractor
import ru.itclover.streammachine.http.utils.ImplicitUtils.RightBiasedEither
import ru.itclover.streammachine.phases.NumericPhases.SymbolNumberExtractor
import InputConf.ThrowableOr


trait InputConf[Event] extends Serializable {
  def sourceId: Int

  def datetimeField: Symbol

  def partitionFields: Seq[Symbol]

  def eventsMaxGapMs: Long

  def fieldsTypesInfo: ThrowableOr[Seq[(Symbol, TypeInformation[_])]]

  // TODO as type-class
  implicit def timeExtractor: ThrowableOr[TimeExtractor[Event]]
  implicit def symbolNumberExtractor: ThrowableOr[SymbolNumberExtractor[Event]]
  implicit def anyExtractor: ThrowableOr[(Event, Symbol) => Any]
}

object InputConf {
  type AnyExtractor[Event] = (Event, Symbol) => Any
  type ThrowableOr[T] = Either[Throwable, T]
  type ThrowableOrTypesInfo = Either[Throwable, Seq[(Symbol, TypeInformation[_])]]
}
