package ru.itclover.tsp.core

import cats.Semigroup

/**
  * Represents found pattern
  *
  * @param id from input conf
  * @param maxWindowMs maximum time-window (accum, aggregation) inside
  * @param segment bounds of incident
  * @param forwardedFields which fields need to push to sink
  */
case class Incident(
  id: String,
  patternId: Int,
  maxWindowMs: Long,
  segment: Segment,
  patternUnit: Int,
  patternSubunit: Int,
) extends Product
    with Serializable

object IncidentInstances {

  implicit def semigroup: Semigroup[Incident] = new Semigroup[Incident] {
    override def combine(a: Incident, b: Incident): Incident = {
      val from =
        if (a.segment.from.toMillis > b.segment.from.toMillis) b.segment.from
        else a.segment.from
      val to =
        if (a.segment.to.toMillis > b.segment.to.toMillis) a.segment.to
        else b.segment.to
      Incident(
        b.id,
        b.patternId,
        b.maxWindowMs,
        Segment(from, to),
        b.patternUnit,
        b.patternSubunit,
      )
    }
  }
}
