package ru.itclover.tsp.mappers

import ru.itclover.tsp.core.io.{Decoder, Extractor}
import ru.itclover.tsp.core.{Incident, Segment}

final case class ToIncidentsMapper[E, EKey, EItem](
  patternId: String,
  forwardedFields: Seq[(String, EKey)],
  payload: Seq[(String, Any)],
  sessionWindowMs: Long,
  partitionFields: Seq[EKey]
)(implicit extractor: Extractor[E, EKey, EItem], decoder: Decoder[EItem, Any]) {
  private val partitionFieldsExtractors = partitionFields.map(f => f -> extractor[Any](f)(decoder))
  private val forwardedFieldsExtractors = forwardedFields.map { case (name, k) => name -> extractor[Any](k)(decoder) }

  def apply(event: E): Segment => Incident = {
    val incidentId = s"P#$patternId;" + partitionFieldsExtractors.map { case (a, b) => a -> b.apply(event) }.mkString
    val extractedFields = forwardedFieldsExtractors.map { case (name, k) => name -> k.apply(event) }
    segment => Incident(incidentId, patternId, sessionWindowMs, segment, extractedFields, payload)
  }
}
