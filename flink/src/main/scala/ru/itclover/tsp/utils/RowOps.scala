package ru.itclover.tsp.utils

import java.time.Instant

import org.apache.flink.types.Row
import ru.itclover.tsp.core.io.{Decoder, Extractor, TimeExtractor}
import ru.itclover.tsp.core.{Time => CoreTime}

import scala.util.Try

object RowOps {
  implicit class RowOps(private val row: Row) extends AnyVal {

    def getFieldOrThrow(i: Int): AnyRef =
      if (row.getArity > i) row.getField(i)
      else throw new RuntimeException(s"Cannot extract $i from row ${row.mkString}")

    def mkString(sep: String): String = "Row(" + (0 until row.getArity).map(row.getField).mkString(sep) + ")"

    def mkString: String = mkString(", ")
  }

  case class RowTsTimeExtractor(timeIndex: Int, tsMultiplier: Double, fieldId: Symbol) extends TimeExtractor[Row] {

    def apply(r: Row) = {
      val millis = r.getField(timeIndex) match {
        case d: java.lang.Double => (d * tsMultiplier).toLong
        case f: java.lang.Float  => (f * tsMultiplier).toLong
        case n: java.lang.Number => (n.doubleValue() * tsMultiplier).toLong
        case null                => 0L // TODO: Where can nulls come from?
        case x                   => sys.error(s"Cannot parse time `$x` from field $fieldId, should be number of millis since 1.1.1970")
      }
      CoreTime(toMillis = millis)
    }
  }

  case class RowIsoTimeExtractor(timeIndex: Int, fieldId: Symbol) extends TimeExtractor[Row] {

    def apply(r: Row) = {
      val isoTime = r.getField(timeIndex).toString
      if (isoTime == null || isoTime == "")
        sys.error(s"Cannot parse time `$isoTime` from field $fieldId, should be in ISO 8601 format")
      CoreTime(toMillis = Instant.parse(isoTime).toEpochMilli)
    }
  }

  case class RowSymbolExtractor(fieldIdxMap: Map[Symbol, Int]) extends Extractor[Row, Symbol, Any] {
    def apply[T](r: Row, s: Symbol)(implicit d: Decoder[Any, T]): T = d(r.getField(fieldIdxMap(s)))
  }

  case class RowIdxExtractor() extends Extractor[Row, Int, Any] {
    def apply[T](r: Row, i: Int)(implicit d: Decoder[Any, T]): T = d(r.getFieldOrThrow(i))
  }
}
