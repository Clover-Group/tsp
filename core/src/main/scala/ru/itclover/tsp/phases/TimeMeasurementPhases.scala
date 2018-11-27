package ru.itclover.tsp.phases

import ru.itclover.tsp.core.{Pattern, PatternResult}
import com.typesafe.scalalogging.Logger

case class PatternStats(time: Long, calls: Long)

object TimeMeasurementPhases {
  case class TimeMeasurementPattern[Event, State, T](
    innerPattern: Pattern[Event, State, T],
    patternName: String
  ) extends Pattern[Event, (State, PatternStats), T] {
    override def initialState: (State, PatternStats) = (innerPattern.initialState, PatternStats(0L, 0L))

    def timePerCall: Double = 0.0 //time / calls.toDouble

    val logger = Logger(s"TimeMeasurement-[$patternName]")

    override def apply(v1: Event, v2: (State, PatternStats)): (PatternResult[T], (State, PatternStats)) = {
      val start = System.nanoTime
      val result = innerPattern.apply(v1, v2._1)
      val end = System.nanoTime
      val (time, calls) = (v2._2.time + (end - start), v2._2.calls + 1)
      (result._1, (result._2, PatternStats(time, calls)))
    }

    override def format(event: Event): String = s"TimeMeasurement(${innerPattern.format(event)})"
  }
}
