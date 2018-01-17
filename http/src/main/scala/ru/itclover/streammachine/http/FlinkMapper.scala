package ru.itclover.streammachine.http

import java.sql.Timestamp

import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import ru.itclover.streammachine.SegmentResultsMapper
import ru.itclover.streammachine.core.Aggregators.Segment
import ru.itclover.streammachine.core.PhaseParser
import ru.itclover.streammachine.core.Time.TimeExtractor
import ru.itclover.streammachine.http.utils.EvalUtils

//object FlinkMapper {
//  def phaseFlatMap(phaseCode: String, fieldsIndexesMap: Map[Symbol, Int], row: Row, collector: Collector[Segment]) = {
//    val timeExtractor: TimeExtractor[Row] = new TimeExtractor[Row] {
//      override def apply(v1: Row) = {
//        v1.getField(1).asInstanceOf[Timestamp]
//      }
//    }
//
//    val phase: PhaseParser[Row, Any, Any] = EvalUtils.evalPhaseUsingRowExtractors(phaseCode, 0, fieldsIndexesMap)
//    FlinkStateCodeMachineMapper(phase, segmentMapper(phase, timeExtractor)).flatMap(row, collector)
//  }
//  def segmentMapper[Event, PhaseOut](p: PhaseParser[Event, _, PhaseOut], te: TimeExtractor[Event]) =
//    SegmentResultsMapper[Event, PhaseOut]()(te)
//
//}

