package ru.itclover.streammachine.transformers

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.types.Row
import org.apache.flink.streaming.api.scala._
import ru.itclover.streammachine.core.PhaseParser
import ru.itclover.streammachine.{EvalUtils, SegmentResultsMapper, ToRowResultMapper}
import ru.itclover.streammachine.core.Time.TimeExtractor
import ru.itclover.streammachine.io.input.{InputConf, JDBCInputConf, RawPattern}
import ru.itclover.streammachine.io.output.RowSchema
import ru.itclover.streammachine.DataStreamUtils.DataStreamOps
import ru.itclover.streammachine.phases.NumericPhases.SymbolNumberExtractor
import ru.itclover.streammachine.http.utils.ImplicitUtils.RightBiasedEither


object PatternsSearchStages {
  def findInRows(stream: DataStream[Row], inputConf: InputConf, patterns: Seq[RawPattern], rowSchema: RowSchema)
                (implicit rowTypeInfo: TypeInformation[Row], streamEnv: StreamExecutionEnvironment) =
  inputConf.fieldsTypesInfo flatMap { fieldsTypesInfo =>
    val fieldsIdxMap = fieldsTypesInfo.map(_._1).zipWithIndex.toMap // For mapping to Row indexes
    val timeInd = fieldsIdxMap(inputConf.datetimeFieldName)

    implicit val timeExtractor: TimeExtractor[Row] = new TimeExtractor[Row] {
      override def apply(event: Row) = event.getField(timeInd).asInstanceOf[Double]
    }
    implicit val symbolNumberExtractorRow: SymbolNumberExtractor[Row] = new SymbolNumberExtractor[Row] {
      override def extract(event: Row, symbol: Symbol): Double = event.getField(fieldsIdxMap(symbol)) match {
        case d: java.lang.Double => d.doubleValue()
        case f: java.lang.Float => f.floatValue().toDouble
        case err => throw new ClassCastException(s"Cannot cast value $err to float or double.")
      }
    }
    implicit val anyExtractor: (Row, Symbol) => Any = (event: Row, name: Symbol) => event.getField(fieldsIdxMap(name))

    for {
      // Try to find free fields for nullIndex excluding time and partitions indexes.
      nullIndex <- findNullIndex(fieldsIdxMap, timeInd +: inputConf.partitionFieldNames.map(fieldsIdxMap))
      partitionIndexes = inputConf.partitionFieldNames.map(fieldsIdxMap)
    } yield {
      val patternsMappers = patterns.map { pattern =>
        def packInMapper = SegmentResultsMapper[Row, Any]() andThen
          new ToRowResultMapper[Row](inputConf.sourceId, rowSchema, pattern)
        val compilePhase = getPhaseCompiler(pattern.sourceCode, inputConf.datetimeFieldName, fieldsIdxMap)
        new FlinkPatternMapper(compilePhase, packInMapper, inputConf.eventsMaxGapMs, new Row(0), isTerminal(nullIndex))
            .asInstanceOf[RichStatefulFlatMapper[Row, Any, Row]]
      }
      stream.keyBy(e => partitionIndexes.map(e.getField).mkString)
            .flatMapAll(patternsMappers)(rowSchema.getTypeInfo)
            .name("Patterns searching stage")
    }
  }

  private def getPhaseCompiler(code: String, timestampField: Symbol, fieldIndexesMap: Map[Symbol, Int]) =
  { classLoader: ClassLoader =>
    val evaluator = new EvalUtils.Eval(classLoader)
    evaluator.apply[(PhaseParser[Row, Any, Any])](
      EvalUtils.composePhaseCodeUsingRowExtractors(code, timestampField, fieldIndexesMap)
    )
  }

  private def isTerminal(nullInd: Int) = { row: Row =>
    row.getArity > nullInd && row.getField(nullInd) == null
  }

  private def findNullIndex(fieldsIdxMap: Map[Symbol, Int], excludedIdx: Seq[Int]) = {
    fieldsIdxMap.find {
      case (_, ind) => !excludedIdx.contains(ind)
    } match {
      case Some((_, nullInd)) => Right(nullInd)
      case None =>
        Left(new IllegalArgumentException(s"Fail to compute nullIndex, query contains only date and partition cols."))
    }
  }
}
