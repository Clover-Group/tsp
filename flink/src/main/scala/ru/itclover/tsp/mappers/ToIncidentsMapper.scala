package ru.itclover.tsp.mappers

import ru.itclover.tsp.core.io.{Decoder, Extractor}
import ru.itclover.tsp.core.{Incident, Segment}

final case class ToIncidentsMapper[E, EKey, EItem](
  patternId: Int,
  forwardedFields: Seq[(String, EKey)],
  subunit: Int,
  payload: Seq[(String, String)],
  sessionWindowMs: Long,
  partitionFields: Seq[EKey]
)(implicit extractor: Extractor[E, EKey, EItem], decoder: Decoder[EItem, Any]) {

  def apply(event: E): Segment => Incident = {
    val incidentId = s"P#$patternId;" + partitionFields.map(f => f -> extractor[Any](event, f)).mkString
    val extractedFields = forwardedFields.map { case (name, k) => name -> extractor[Any](event, k).toString }
    segment => Incident(incidentId, patternId, sessionWindowMs, segment, extractedFields, subunit, payload)
  }
}
