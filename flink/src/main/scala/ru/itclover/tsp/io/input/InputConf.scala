package ru.itclover.tsp.io.input

import org.apache.flink.api.common.io.{GenericInputFormat, InputFormat, RichInputFormat}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.core.io.InputSplit
import org.apache.flink.types.Row
import ru.itclover.tsp.core.Time.{TimeExtractor, TimeNonTransformedExtractor}
import ru.itclover.tsp.phases.NumericPhases.{IndexNumberExtractor, SymbolNumberExtractor}
import ru.itclover.tsp.phases.Phases.{AnyExtractor, AnyNonTransformedExtractor}
import ru.itclover.tsp.utils.UtilityTypes.ThrowableOr
import ru.itclover.tsp.io.Exceptions
import ru.itclover.tsp.transformers.StreamSource

trait InputConf[Event] extends Serializable {
  def sourceId: Int

  def datetimeField: Symbol
  def partitionFields: Seq[Symbol]
  def fieldsTypesInfo: ThrowableOr[Seq[(Symbol, TypeInformation[_])]]

  def parallelism: Option[Int]          // Parallelism per each source
  def numParallelSources: Option[Int]   // Number on parallel (separate) sources to be created
  def patternsParallelism: Option[Int]  // Number of parallel branches after source step

  def eventsMaxGapMs: Long
  def defaultEventsGapMs: Long

  def dataTransformation: Option[SourceDataTransformation]

  def createStreamSource: ThrowableOr[StreamSource[Event]]
  
  
  // TODO to StreamSource
  def errOrFieldsIdxMap: ThrowableOr[Map[Symbol, Int]]
  
  implicit def timeExtractor: ThrowableOr[TimeExtractor[Event]]
  implicit def timeNonTransformedExtractor: ThrowableOr[TimeNonTransformedExtractor[Event]]
  implicit def symbolNumberExtractor: ThrowableOr[SymbolNumberExtractor[Event]]
  implicit def indexNumberExtractor: IndexNumberExtractor[Event]
  implicit def anyExtractor: ThrowableOr[AnyExtractor[Event]]
  implicit def anyNonTransformedExtractor: ThrowableOr[AnyNonTransformedExtractor[Event]]
  implicit def keyValExtractor: ThrowableOr[Row => (Symbol, AnyRef)]
}

object InputConf {

  def getRowFieldOrThrow(event: Row, fieldsIdxMap: Map[Symbol, Int], field: Symbol): AnyRef = {
    val ind = fieldsIdxMap.getOrElse(field, Int.MaxValue)
    if (ind >= event.getArity) {
      val available = fieldsIdxMap.map(_._1.toString.tail).mkString(", ")
      throw Exceptions.InvalidRequest(s"There is no sensor `${field.toString.tail}` only `$available`. Event was `$event`")
    }
    event.getField(ind)
  }
  
  def getRowFieldOrThrow(event: Row, index: Int): AnyRef = {
    if (index >= event.getArity) {
      throw Exceptions.InvalidRequest(s"There is no sensorId $index, max index `${event.getArity}`. Event was `$event`")
    }
    event.getField(index)
  }

  def getKVFieldOrThrow(event: Row, keyColumnIndex: Int, valueColumnIndex: Int): (Symbol, AnyRef) = {
    (Symbol(event.getField(keyColumnIndex).toString), event.getField(valueColumnIndex))
  }
}
