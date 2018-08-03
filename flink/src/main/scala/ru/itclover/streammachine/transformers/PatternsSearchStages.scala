package ru.itclover.streammachine.transformers

import cats.data.Validated
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.types.Row
import org.apache.flink.streaming.api.scala._
import ru.itclover.streammachine.core.PhaseParser
import ru.itclover.streammachine._
import ru.itclover.streammachine.core.Time.TimeExtractor
import ru.itclover.streammachine.io.input.{InputConf, JDBCInputConf, RawPattern}
import ru.itclover.streammachine.io.output.{EventSchema, RowSchema}
import ru.itclover.streammachine.DataStreamUtils.DataStreamOps
import ru.itclover.streammachine.newsyntax.SyntaxParser
import ru.itclover.streammachine.phases.NumericPhases.{SymbolExtractor, SymbolNumberExtractor}
import ru.itclover.streammachine.phases.Phases.AnyExtractor
import ru.itclover.streammachine.utils.CollectionsOps._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class PatternsSearchStages {
  import cats.Traverse
  import cats.instances.future._ // for Applicative
  import cats.instances.list._

  val stageName: String = "Patterns search stage"

  // TODO Event type param (after external DSL)
  def findInRows(stream: DataStream[Row], inputConf: InputConf[Row], patterns: Seq[RawPattern],
                 rowSchema: RowSchema)
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
        (pattern.id, new FlinkCompilingPatternMapper(compilePhase, packInMapper, inputConf.eventsMaxGapMs, new Row(0),
          isTerminal(fieldsIdxMap(nullField))).asInstanceOf[RichStatefulFlatMapper[Row, Any, Row]])
      }
      val partitionFields = inputConf.partitionFields // made job code serializable
      val keyedStream = stream.keyBy(e => {
        val extractor = anyExt
        partitionFields.map(extractor(e, _)).mkString
      })

      keyedStream.flatMapAll(patternsMappers.map(_._2))(rowSchema.getTypeInfo).name(stageName)
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

// TODO(D): Why class and object?
/*
object PatternsSearchStages extends PatternsSearchStages {

}

object PatternsSearchStagesDSL extends PatternsSearchStages {
  override protected def getPhaseCompiler(code: String, timestampField: Symbol,
                                          fieldIndexesMap: Map[Symbol, Int])(implicit timeExtractor: TimeExtractor[Row],
                                                                             numberExtractor: SymbolNumberExtractor[Row]):
  ClassLoader => PhaseParser[Row, Any, Any] =
    _ => new SyntaxParser(code).start.run().get
}*/
