package ru.itclover.streammachine.resultmappers

import org.apache.flink.types.Row
import ru.itclover.streammachine
import ru.itclover.streammachine.{ResultMapper, SegmentResultsMapper, ToRowResultMapper}
import ru.itclover.streammachine.core.Time.TimeExtractor
import ru.itclover.streammachine.io.input.RawPattern
import ru.itclover.streammachine.io.output.RowSchema
import ru.itclover.streammachine.phases.Phases.AnyExtractor


trait ResultMappable[InEvent, PhaseOut, OutEvent] {
  def apply(pattern: RawPattern)
           (implicit timeExtractor: TimeExtractor[InEvent], anyExtractor: AnyExtractor[InEvent]): ResultMapper[InEvent, PhaseOut, OutEvent]
}

case class ToRowResultMappers(sourceId: Int, rowSchema: RowSchema)
     extends ResultMappable[Row, Any, Row] {
  def apply(pattern: RawPattern)
           (implicit timeExtractor: TimeExtractor[Row], anyExtractor: AnyExtractor[Row]): ResultMapper[Row, Any, Row] =
    SegmentResultsMapper[Row, Any]() andThen new ToRowResultMapper[Row](sourceId, rowSchema, pattern)
}
