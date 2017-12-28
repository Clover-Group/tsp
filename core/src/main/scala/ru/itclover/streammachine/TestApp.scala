package ru.itclover.streammachine

import java.time.Instant

import ru.itclover.streammachine.core.Aggregators.Timer
import ru.itclover.streammachine.core._
import ru.itclover.streammachine.core.Time._
import ru.itclover.streammachine.phases.Phases.{Assert, Decreasing}

import scala.concurrent.duration._

object TestApp extends App {

  implicit val extractTime: TimeExtractor[Event] = new TimeExtractor[Event] {
    override def apply(v1: Event) = v1.time
  }

  def run[T, Out](rule: PhaseParser[T, _, Out], events: Seq[T]) = {
    val mapResults = FakeMapper[T, Out]()
    events
      .foldLeft(StateMachineMapper(rule, mapResults)) { case (machine, event) => machine(event) }
       .result
  }

  val now = Instant.now()
  val timeSeq = Stream.from(0).map(t => now.plusSeconds(t.toLong))

  val events = 100.to(0, -1).zip(timeSeq).map(Event.tupled)

  val phase =
    Decreasing[Event, Int](_.speed, 250, 50)
  //      .andThen(
  //        Assert[Event](_.speed > 0)
  //          and
  //          Timer[Event]( TimeInterval(10.seconds, 30.seconds))(_.time)
  //      )

  val collector = run(phase, events)

  //  Average[Event](5 seconds, _.speed, 'avgSpeed)

  collector.foreach(println)

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



    val window = new Window {
      override def toMillis: Long = 5000
    }

    type Phase[Event] = PhaseParser[Event, _, _]

    val phase: Phase[Event] = ((e: Event) => e.speed) > 4

    val phase2: Phase[Event] = avg(field('speed), window) > avg('pump, window)

    val phase3 = avg('speed, 5.seconds) >= 5.0 andThen avg('pump, 3.seconds) > 0

    val phase4: Phase[Event] = avg((e: Event) => e.speed, 5.seconds) >= value(5.0)

    val phase5: Phase[Event] = ('speed > 4 & 'pump > 100).timed(more(10.seconds))

    val t: Phase[Event] = 'speed >= 100
    val decr: Phase[Event] = ('speed === 100) andThen (derivation('speed) < 0) until ('speed <= 50)

    val phase6 = 'currentCompressorMotor > 0 &
      ('PAirMainRes <= 7.5 andThen (derivation(avg('PAirMainRes, 5.seconds) ) > 0).timed(more(23.seconds) )
        until 'PAirMainRes >= 8.0 )
  }
}

case class Event(speed: Int, time: Instant)

