package ru.itclover.tsp.spark.utils

import ru.itclover.tsp.core.io.{Decoder, Extractor}
import ru.itclover.tsp.core.{Incident, Segment}

import scala.util.Try

final case class ToIncidentsMapper[E, EKey, EItem](
                                                    patternId: String,
                                                    forwardedFields: Seq[(String, EKey)],
                                                    payload: Seq[(String, String)],
                                                    sessionWindowMs: Long,
                                                    partitionFields: Seq[EKey]
                                                  )(implicit extractor: Extractor[E, EKey, EItem], decoder: Decoder[EItem, Any]) {

  def apply(event: E): Segment => Incident = {
    val incidentId = s"P#$patternId;" + partitionFields.map(f => f -> extractor[Any](event, f)).mkString
    val extractedFields = forwardedFields.map { case (name, k) => name -> Try(extractor[Any](event, k).toString).getOrElse("null") }
    segment => Incident(incidentId, patternId, sessionWindowMs, segment, extractedFields, payload)
  }
}