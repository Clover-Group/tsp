package ru.itclover.tsp.core

import cats.Semigroup
import ru.itclover.tsp.Segment

/**
  * Represents found pattern
  *
  * @param id from input conf
  * @param maxWindowMs maximum time-window (accum, aggregation) inside
  * @param segment bounds of incident
  * @param forwardedFields which fields need to push to sink
  * @param partitionFields by which fields input stream was patiotioned
  */
case class Incident(
  id: String,
  maxWindowMs: Long,
  segment: Segment,
  forwardedFields: Map[Symbol, Any],
  partitionFields: Map[Symbol, Any]
) extends Product
    with Serializable



object IncidentInstances {

  implicit val semigroup = new Semigroup[Incident] {
    override def combine(a: Incident, b: Incident) = {
      val from =
        if (a.segment.from.toMillis > b.segment.from.toMillis) b.segment.from
        else a.segment.from
      val to =
        if (a.segment.to.toMillis > b.segment.to.toMillis) a.segment.to
        else b.segment.to
      Incident(
        a.id,
        a.maxWindowMs,
        Segment(from, to),
        b.forwardedFields,
        b.partitionFields
      )
    }
  }
}