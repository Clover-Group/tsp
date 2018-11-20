package ru.itclover.tsp.utils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.metrics.Counter
import ru.itclover.tsp.core.{Pattern, PatternResult}
import com.typesafe.scalalogging.Logger

object TimeMeasurementPhases {
  case class TimeMeasurementPattern[Event, State, T](innerPattern: Pattern[Event, State, T],
                                                     mapper: RichMapFunction[_, _],
                                                     patternName: String)

      extends Pattern[Event, State, T] {
    override def initialState: State = innerPattern.initialState

    lazy val timeCounter: Counter = mapper.getRuntimeContext.getMetricGroup.counter(s"ExtraTime$patternName")
    lazy val eventCounter: Counter = mapper.getRuntimeContext.getMetricGroup.counter(s"ExtraEvents$patternName")
    val logger = Logger(s"TimeMeasurement.$patternName")
    logger.error(s"Initialised pattern timing for [$patternName]")

    def timePerEvent: Double = timeCounter.getCount / eventCounter.getCount.toDouble

    override def apply(v1: Event, v2: State): (PatternResult[T], State) = {
      val start = System.nanoTime
      val result = innerPattern.apply(v1, v2)
      val end = System.nanoTime
      timeCounter.inc(end - start)
      eventCounter.inc(1)
      logger.error(s"[$patternName] running for ${timeCounter.getCount} ns with ${eventCounter.getCount} events")
      result
    }

    override def format(event: Event): String = s"TimeMeasurement(${innerPattern.format(event)})"
  }
}
