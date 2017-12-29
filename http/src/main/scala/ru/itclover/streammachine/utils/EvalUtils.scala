package ru.itclover.streammachine.utils

import com.twitter.util.Eval
import org.apache.flink.types.Row
import ru.itclover.streammachine.core.PhaseParser
import ru.itclover.streammachine.phases.Phases.Phase

import scala.reflect.ClassTag


object EvalUtils {



   def evalPhaseUsingRowExtractors(phaseCode: String, timestampFieldIndex: Int, fieldsIndexesMap: Map[Symbol, Int]) =

     new com.twitter.util.Eval().apply[PhaseParser[Row, _, _]](s"""
        |import ru.itclover.streammachine.core.Aggregators._
        |import ru.itclover.streammachine.core.AggregatingPhaseParser._
        |import ru.itclover.streammachine.core.NumericPhaseParser._
        |import ru.itclover.streammachine.core.Time._
        |import ru.itclover.streammachine.core.PhaseParser
        |import Predef.{any2stringadd => _, _}
        |import ru.itclover.streammachine.phases.Phases._
        |import org.apache.flink.types.Row
        |
        |implicit val symbolNumberExtractorRow: SymbolNumberExtractor[Row] = new SymbolNumberExtractor[Row] {
        |  val fieldsIndexesMap: Map[Symbol, Int] = ${fieldsIndexesMap.toString}
        |
        |  override def extract(event: Row, symbol: Symbol) = {
        |    event.getField(fieldsIndexesMap(symbol)).asInstanceOf[Double]
        |  }
        |}
        |implicit val timeExtractor: TimeExtractor[Row] = new TimeExtractor[Row] {
        |  override def apply(v1: Row) = {
        |    v1.getField($timestampFieldIndex).asInstanceOf[java.sql.Timestamp]
        |  }
        |}
        |
        |val phase = $phaseCode
        |phase
      """.stripMargin)
}
