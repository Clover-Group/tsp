package ru.itclover.tsp.io.input

import org.apache.flink.api.common.io.{GenericInputFormat, InputFormat, RichInputFormat}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.core.io.InputSplit
import org.apache.flink.types.Row
import ru.itclover.tsp.core.Time.TimeExtractor
import ru.itclover.tsp.phases.NumericPhases.SymbolNumberExtractor
import ru.itclover.tsp.phases.Phases.AnyExtractor
import ru.itclover.tsp.utils.UtilityTypes.ThrowableOr
import ru.itclover.tsp.io.Exceptions

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

  // TODO to StreamSource
  implicit def timeExtractor: ThrowableOr[TimeExtractor[Event]]
  implicit def symbolNumberExtractor: ThrowableOr[SymbolNumberExtractor[Event]]
  implicit def anyExtractor: ThrowableOr[AnyExtractor[Event]]
}

object InputConf {

  def getRowFieldOrThrow(event: Row, fieldsIdxMap: Map[Symbol, Int], field: Symbol): AnyRef = {
    val ind = fieldsIdxMap.getOrElse(field, Int.MaxValue)
    if (ind >= event.getArity) {
      val available = fieldsIdxMap.map(_._1.toString.tail).mkString(", ")
      throw Exceptions.InvalidRequest(s"There is no sensor `${field.toString.tail}` only `$available`")
    }
    event.getField(ind)
  }
}
