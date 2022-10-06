package ru.itclover.tsp.streaming.mappers

import ru.itclover.tsp.core.io.{Decoder, Extractor}
import ru.itclover.tsp.core.{Incident, Segment}

import java.util.UUID
import scala.util.Try

final case class ToIncidentsMapper[E, EKey, EItem](
  patternId: Int,
  unitIdField: EKey,
  subunit: Int,
  patternMetadata: Map[String, String],
  sessionWindowMs: Long,
  partitionFields: Seq[EKey]
)(implicit extractor: Extractor[E, EKey, EItem], decoder: Decoder[EItem, Any]) {

  def apply(event: E): Segment => Incident = {
    val incidentId = s"P#$patternId;" + partitionFields.map(f => f -> extractor[Any](event, f)).mkString
    val unit = Try(extractor[Any](event, unitIdField).toString.toInt).getOrElse(Int.MinValue)
    segment =>
      Incident(
        incidentId,
        UUID.randomUUID(),
        patternId,
        sessionWindowMs,
        segment,
        unit,
        subunit,
        patternMetadata
      )
  }
}
