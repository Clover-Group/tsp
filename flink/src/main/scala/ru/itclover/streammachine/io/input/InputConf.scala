package ru.itclover.streammachine.io.input

import org.apache.flink.api.common.io.{GenericInputFormat, InputFormat, RichInputFormat}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.core.io.InputSplit
import org.apache.flink.types.Row
import ru.itclover.streammachine.core.Time.TimeExtractor
import ru.itclover.streammachine.phases.NumericPhases.SymbolNumberExtractor
import ru.itclover.streammachine.phases.Phases.AnyExtractor
import ru.itclover.streammachine.utils.UtilityTypes.ThrowableOr


trait InputConf[Event] extends Serializable {
  def sourceId: Int

  def datetimeField: Symbol
  def partitionFields: Seq[Symbol]
  def fieldsTypesInfo: ThrowableOr[Seq[(Symbol, TypeInformation[_])]]

  def parallelism: Option[Int]
  def eventsMaxGapMs: Long

  // TODO to StreamSource
  implicit def timeExtractor: ThrowableOr[TimeExtractor[Event]]
  implicit def symbolNumberExtractor: ThrowableOr[SymbolNumberExtractor[Event]]
  implicit def anyExtractor: ThrowableOr[AnyExtractor[Event]]
}
