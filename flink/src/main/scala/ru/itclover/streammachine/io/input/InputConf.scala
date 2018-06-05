package ru.itclover.streammachine.io.input

import org.apache.flink.api.common.typeinfo.TypeInformation
import ru.itclover.streammachine.core.Time.TimeExtractor
import ru.itclover.streammachine.phases.NumericPhases.SymbolNumberExtractor
import ru.itclover.streammachine.utils.UtilityTypes.ThrowableOr


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
