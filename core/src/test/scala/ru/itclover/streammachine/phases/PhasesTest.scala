package ru.itclover.streammachine.phases

import java.time.Instant
import org.scalatest.{FunSuite, Matchers}
import ru.itclover.streammachine.Event
import ru.itclover.streammachine.core.AggregatingPhaseParser.derivation
import ru.itclover.streammachine.core.{Derivation, NumericPhaseParser}
import ru.itclover.streammachine.core.NumericPhaseParser.{SymbolNumberExtractor, field}

class PhasesTest extends FunSuite with Matchers {

  case class Event(speed: Int, time: Instant)

  implicit val symbolNumberExtractorEvent = new SymbolNumberExtractor[Event] {
      override def extract(event: Event, symbol: Symbol) = {
        symbol match {
          case 'speed => event.speed
          case _ => sys.error(s"No field $symbol in $event")
        }
      }
    }

  test("Derivation phase should work") {
    val speed = field('speed)
    val initialState = (speed.initialState, None)
    val (result1, state1) = derivation(speed).apply(Event(100, Instant.now()), initialState)
    val (result2, state2) = derivation(speed).apply(Event(200, Instant.now()), state1)
    result2.isTerminal shouldEqual true
    for {
      derivative <- result2
    } yield {
      derivative should be > 0.0
      derivative shouldEqual (100.0 +- 0.000001)
    }
  }
}
