package ru.itclover.tsp.utils

import org.apache.flink.types.Row
import ru.itclover.tsp.io.{Decoder, Extractor}

object RowOps {
  implicit class RowOps(val row: Row) extends AnyVal {
    def getFieldOrThrow(i: Int): AnyRef = 
      if (row.getArity > i) row.getField(i) 
      else throw new RuntimeException(s"Cannot extract $i from row ${row.mkString}")
    
    def mkString(sep: String): String = "Row(" + (0 until row.getArity).map(row.getField).mkString(sep) + ")"
    
    def mkString: String = mkString(", ")
  }
  
  
  case class RowSymbolExtractor(fieldIdxMap: Map[Symbol, Int]) extends Extractor[Row, Symbol, Any] {
    def apply[T](r: Row, s: Symbol)(implicit d: Decoder[Any, T]): T = d(r.getField(fieldIdxMap(s)))
  }
  
  case class RowIdxExtractor() extends Extractor[Row, Int, Any] {
    def apply[T](r: Row, i: Int)(implicit d: Decoder[Any, T]): T = d(r.getFieldOrThrow(i))
  }
}
