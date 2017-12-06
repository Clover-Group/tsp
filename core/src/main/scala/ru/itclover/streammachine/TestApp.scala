package ru.itclover.streammachine

import java.time.Instant

import ru.itclover.streammachine.core.Aggregators.Timer
import ru.itclover.streammachine.core.{PhaseParser, TimeInterval, Window}
import ru.itclover.streammachine.core.Time._
import ru.itclover.streammachine.phases.Phases.{Assert, Decreasing}

import scala.concurrent.duration._

object TestApp extends App {

  val now = Instant.now()
  val timeSeq = Stream.from(0).map(t => now.plusSeconds(t.toLong))

  val events = 100.to(0, -1).zip(timeSeq).map(Event.tupled)

  val phase =
    Decreasing[Event, Int](_.speed, 250, 50)
      .andThen(
        Assert[Event](_.speed > 0)
          and
          Timer[Event]( TimeInterval(10.seconds, 30.seconds))(_.time)
      )

  val collector = events.foldLeft(StateMachineMapper(phase)) { case (mapper, event) => mapper(event) }

  //  Average[Event](5 seconds, _.speed, 'avgSpeed)

  collector.result.foreach(println)

  {
    import core.Aggregators._
    import core.AggregatingPhaseParser._
    import core.NumericPhaseParser._
    import Predef.{any2stringadd => _, _}

    implicit val symbolNumberExtractorEvent = new SymbolNumberExtractor[Event] {
      override def extract(event: Event, symbol: Symbol) = {
        symbol match {
          case 'speed => event.speed
          case _ => sys.error(s"No field $symbol in $event")
        }
      }
    }


    implicit val timeExtractor = new TimeExtractor[Event] {
      override def apply(v1: Event) = v1.time
    }


    val window = new Window {
      override def toMillis: Long = 5000
    }
    type Phase[Event] = PhaseParser[Event, _, _]

    val phase: Phase[Event] = ((e: Event) => e.speed) > 4

    val phase2: Phase[Event] = avg('speed, window) > avg('pump, window)

    val phase3 = avg('speed, 5.seconds) >= 5.0 andThen avg('pump, 3.seconds) > 0

    val phase4: Phase[Event] = avg((e: Event) => e.speed, 5.seconds) >= value(5.0)

    val phase5: Phase[Event] = ('speed > 4 & 'pump > 100).timed(more(10.seconds))


  }
}

case class Event(speed: Int, time: Instant)

