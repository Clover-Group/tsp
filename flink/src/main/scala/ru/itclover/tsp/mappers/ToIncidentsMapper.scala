package ru.itclover.tsp.mappers

import ru.itclover.tsp.core.Incident
import ru.itclover.tsp.io.{Decoder, Extractor}
import ru.itclover.tsp.{ResultMapper, Segment}
import ru.itclover.tsp.core.PatternResult.{Failure, Success, TerminalResult}

final case class ToIncidentsMapper[E, EKey, EItem](
  patternId: String,
  forwardedFields: Seq[(String, EKey)],
  payload: Seq[(String, Any)],
  sessionWindowMs: Long,
  partitionFields: Seq[EKey]
)(implicit extractor: Extractor[E, EKey, EItem], decoder: Decoder[EItem, Any]) {

  def apply(event: E, results: Seq[TerminalResult[Segment]]): Seq[Incident] =
    results flatMap {
      case Success(segment) =>
        Seq(
          Incident(
            s"P#${patternId};" + partitionFields.map(f => f -> extractor[Any](event, f)).mkString,
            patternId,
            sessionWindowMs,
            segment,
            forwardedFields.map { case (name, k) => name -> extractor[Any](event, k) },
            payload
          )
        )
      case _: Failure => Seq.empty
    }
}
