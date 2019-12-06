package ru.itclover.tsp.spark.utils

import ru.itclover.tsp.core.io.{Decoder, Extractor}
import ru.itclover.tsp.core.{Incident, Segment}

final case class ToIncidentsMapper[E, EKey, EItem](
                                                    patternId: String,
                                                    forwardedFields: Seq[(String, EKey)],
                                                    payload: Seq[(String, Any)],
                                                    sessionWindowMs: Long,
                                                    partitionFields: Seq[EKey]
                                                  )(implicit extractor: Extractor[E, EKey, EItem], decoder: Decoder[EItem, Any]) {

  def apply(event: E): Segment => Incident = {
    val incidentId = s"P#$patternId;" + partitionFields.map(f => f -> extractor[Any](event, f)).mkString
    val extractedFields = forwardedFields.map { case (name, k) => name -> extractor[Any](event, k) }
    segment => Incident(incidentId, patternId, sessionWindowMs, segment, extractedFields, payload)
  }
}