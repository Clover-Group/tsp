package ru.itclover.streammachine.io.input.source

import org.apache.flink.api.common.io.RichInputFormat


/**
  * Source information requested by configs.
  * @tparam Event - type of data stream events.
  */
trait SourceInfo[Event] {
  def fieldsNames(): Seq[Symbol]

  def datetimeFieldName: Symbol

  def inputFormat: RichInputFormat[Event, _]
}
