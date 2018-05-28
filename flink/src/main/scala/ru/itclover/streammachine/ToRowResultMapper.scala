package ru.itclover.streammachine

import java.time._
import java.sql.Timestamp
import java.time.DateTimeException
import org.apache.flink.types.Row
import ru.itclover.streammachine.aggregators.AggregatorPhases.Segment
import ru.itclover.streammachine.core.PhaseResult.{Failure, Success, TerminalResult}
import ru.itclover.streammachine.core.Time.TimeExtractor
import ru.itclover.streammachine.io.input.RawPattern
import ru.itclover.streammachine.io.output.RowSchema


/**
  * Packer of PhaseOut into [[org.apache.flink.types.Row]]
  * @tparam Event - inner Event
  */
class ToRowResultMapper[Event](sourceId: Int, schema: RowSchema, pattern: RawPattern)
                              (implicit timeExtractor: TimeExtractor[Row], extractAny: (Event, Symbol) => Any)
  extends ResultMapper[Event, Segment, Row] {

  // TODO: replace mappers with result_phase on parser like `SELECT [result_phase] WHERE [phase]`
  // Segments closes on next after last success event, hence forwarded fields should be got from prev successful Event.
  var prevEvent: Option[Event] = None

  override def apply(event: Event, results: Seq[TerminalResult[Segment]]) = {
    val packedEvent = results map {
      case (Success(segment)) => {
        val resultRow = new Row(schema.fieldsCount)
        resultRow.setField(schema.sourceIdInd, sourceId)
        resultRow.setField(schema.patternIdInd, pattern.id)
        resultRow.setField(schema.appIdInd, schema.appIdFieldVal._2)
        resultRow.setField(schema.beginInd, segment.from.toMillis / 1000.0)
        resultRow.setField(schema.endInd, segment.to.toMillis / 1000.0)
        resultRow.setField(schema.processingTimeInd, nowInUtcMillis)

        val forwardedFields = pattern.forwardedFields ++ schema.forwardedFields
        val payload = forwardedFields.map(f => f.toString.tail -> extractAny(prevEvent.getOrElse(event), f)) ++
          pattern.payload.toSeq
        resultRow.setField(schema.contextInd, payloadToJson(payload))

        Success(resultRow)
      }
      case f@Failure(msg) => f
    }
    prevEvent = Some(event)
    packedEvent
  }

  def nowInUtcMillis: Double = {
    val zonedDt = ZonedDateTime.of(LocalDateTime.now, ZoneId.systemDefault)
    val utc = zonedDt.withZoneSameInstant(ZoneId.of("UTC"))
    Timestamp.valueOf(utc.toLocalDateTime).getTime / 1000.0
  }

  def payloadToJson(payload: Seq[(String, Any)]): String = {
    payload.map {
      case (fld, value) if value.isInstanceOf[String] => s""""${fld}":"${value}""""
      case (fld, value) => s""""${fld}":$value"""
    } mkString("{", ",", "}")
  }
}
