package ru.itclover.tsp.utils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.metrics.Counter
import ru.itclover.tsp.core.{Pattern, PatternResult}
import com.typesafe.scalalogging.Logger

object TimeMeasurementPhases {
  case class TimeMeasurementPattern[Event, State, T](
    innerPattern: Pattern[Event, State, T],
    mapper: RichMapFunction[_, _],
    patternName: String
  ) extends Pattern[Event, State, T] {
    override def initialState: State = innerPattern.initialState

    var time = 0L
    var calls = 0L

    def timePerCall: Double = time / calls.toDouble

    val logger = Logger(s"TimeMeasurement-[$patternName]")

    override def apply(v1: Event, v2: State): (PatternResult[T], State) = {
      val start = System.nanoTime
      val result = innerPattern.apply(v1, v2)
      val end = System.nanoTime
      //timeCounter.inc(end - start)
      //eventCounter.inc(1)
      //logger.error(s"[$patternName] running for ${timeCounter.getCount} ns with ${eventCounter.getCount} events")
      time += (end - start)
      calls += 1
      result._1 match {
        case _: PatternResult.TerminalResult[_] =>
          logger.warn(s"[$patternName] terminated after $calls calls in $time ns ($timePerCall per call)")
        case PatternResult.Stay         =>
      }
      result
    }

    override def format(event: Event): String = s"TimeMeasurement(${innerPattern.format(event)})"
  }
}
