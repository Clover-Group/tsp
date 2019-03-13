package ru.itclover.tsp.mappers

import ru.itclover.tsp.core.Incident
import ru.itclover.tsp.io.{Decoder, Extractor}
import ru.itclover.tsp.{Segment}
import ru.itclover.tsp.core.PatternResult.{Failure, Success, TerminalResult}

final case class ToIncidentsMapper[E, EKey, EItem](
  patternId: String,
  forwardedFields: Seq[(String, EKey)],
  payload: Seq[(String, Any)],
  sessionWindowMs: Long,
  partitionFields: Seq[EKey]
)(implicit extractor: Extractor[E, EKey, EItem], decoder: Decoder[EItem, Any]) {

  def apply(event: E, results: Seq[Segment]): Seq[Incident] = {
    val incidentId = s"P#$patternId;" + partitionFields.map(f => f -> extractor[Any](event, f)).mkString
    val extractedFields = forwardedFields.map { case (name, k) => name -> extractor[Any](event, k) }
    results flatMap { segment =>
      Seq(
        Incident(
          incidentId,
          patternId,
          sessionWindowMs,
          segment,
          extractedFields,
          payload
        )
      )
    }
  }
}
