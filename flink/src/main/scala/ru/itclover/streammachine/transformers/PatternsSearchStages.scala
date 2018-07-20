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
import ru.itclover.streammachine.newsyntax.{PhaseBuilder, SyntaxParser}
import ru.itclover.streammachine.phases.NumericPhases.{SymbolExtractor, SymbolNumberExtractor}
import ru.itclover.streammachine.utils.CollectionsOps._


class PatternsSearchStages {

  // TODO Event type param (after external DSL)
  def findInRows(stream: DataStream[Row], inputConf: InputConf[Row], patterns: Seq[RawPattern], rowSchema: RowSchema)
                (implicit rowTypeInfo: TypeInformation[Row],
                 streamEnv: StreamExecutionEnvironment): Either[Throwable, DataStream[Row]] =
    for {
      fieldsTypesInfo <- inputConf.fieldsTypesInfo
      fieldsIdxMap = fieldsTypesInfo.map(_._1).zipWithIndex.toMap // For mapping to Row indexes
      timeExtractor <- inputConf.timeExtractor // TODO: to context bounds of Event (as type-class from routes InputConf)
      numberExtractor <- inputConf.symbolNumberExtractor
      anyExtractor <- inputConf.anyExtractor
      nullField <- findNullField(fieldsIdxMap.keys.toSeq, inputConf.datetimeField +: inputConf.partitionFields)
    } yield {
      implicit val (timeExt, numberExt, anyExt) = (timeExtractor, numberExtractor, anyExtractor)

      val patternsMappers = patterns.map { pattern =>
        def packInMapper = SegmentResultsMapper[Row, Any]() andThen
          new ToRowResultMapper[Row](inputConf.sourceId, rowSchema, pattern)

        val compilePhase = getPhaseCompiler(pattern.sourceCode, inputConf.datetimeField, fieldsIdxMap)
        (pattern.id, new FlinkPatternMapper(compilePhase, packInMapper, inputConf.eventsMaxGapMs, new Row(0),
          isTerminal(fieldsIdxMap(nullField))).asInstanceOf[RichStatefulFlatMapper[Row, Any, Row]])
      }
      val partitionFields = inputConf.partitionFields // make job code serializable
      val keyedStream = stream.keyBy(e => {
        val extractor = anyExt
        partitionFields.map(extractor(e, _)).mkString
      })

      keyedStream.flatMapAll(patternsMappers.map(_._2))(rowSchema.getTypeInfo).name(s"Patterns search stage")
    }

  protected def getPhaseCompiler(code: String, timestampField: Symbol, fieldIndexesMap: Map[Symbol, Int])
                                (implicit timeExtractor: TimeExtractor[Row],
                                 numberExtractor: SymbolNumberExtractor[Row]): ClassLoader => PhaseParser[Row, Any, Any] = {
    classLoader: ClassLoader =>
      val evaluator = new EvalUtils.Eval(classLoader)
      evaluator.apply[(PhaseParser[Row, Any, Any])](
        EvalUtils.composePhaseCodeUsingRowExtractors(code, timestampField, fieldIndexesMap)
      )
  }

  private def isTerminal(nullInd: Int) = { row: Row =>
    row.getArity > nullInd && row.getField(nullInd) == null
  }

  private def findNullField(allFields: Seq[Symbol], excludedFields: Seq[Symbol]) = {
    allFields.find {
      field => !excludedFields.contains(field)
    } match {
      case Some(nullField) => Right(nullField)
      case None =>
        Left(new IllegalArgumentException(s"Fail to compute nullIndex, query contains only date and partition cols."))
    }
  }
}

object PatternsSearchStages extends PatternsSearchStages {

}

object PatternsSearchStagesDSL extends PatternsSearchStages {
  override protected def getPhaseCompiler(code: String, timestampField: Symbol,
                                          fieldIndexesMap: Map[Symbol, Int])(implicit timeExtractor: TimeExtractor[Row],
                                                                             numberExtractor: SymbolNumberExtractor[Row]):
  ClassLoader => PhaseParser[Row, Any, Any] =
    _ => new PhaseBuilder[Row].build(new SyntaxParser(code).start.run().get).asInstanceOf[PhaseParser[Row, Any, Any]]
}