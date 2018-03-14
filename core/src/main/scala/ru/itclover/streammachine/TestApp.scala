package ru.itclover.streammachine

import java.time.Instant

import ru.itclover.streammachine.aggregators.AggregatorPhases.Derivation
import ru.itclover.streammachine.core.Time._
import ru.itclover.streammachine.core._
import ru.itclover.streammachine.core.PhaseParser.Functions._
import ru.itclover.streammachine.phases.NumericPhases.SymbolParser
import ru.itclover.streammachine.phases.NumericPhases._
import ru.itclover.streammachine.phases.Phases.Decreasing

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
    import Predef.{any2stringadd => _, _}

    implicit val symbolNumberExtractorEvent: SymbolNumberExtractor[Event] = new SymbolNumberExtractor[Event] {
      override def extract(event: Event, symbol: Symbol) = {
        symbol match {
          case 'speed => event.speed
          case _ => sys.error(s"No field $symbol in $event")
        }
      }
    }

    case class Appevent(`type`: String, created: Long)

    implicit val symbolNumberExtractorAppevent: SymbolExtractor[Appevent, String] = new SymbolExtractor[Appevent, String] {
      override def extract(event: Appevent, symbol: Symbol): String = {
        symbol match {
          case 'eventType => event.`type`
          case _ => sys.error(s"No field $symbol in $event")
        }
      }
    }

    implicit val extractTimeAppevent: TimeExtractor[Appevent] = new TimeExtractor[Appevent] {
      override def apply(v1: Appevent): Time = v1.created
    }


    val window = Window(toMillis = 500)

    type Phase[Event] = PhaseParser[Event, _, _]

    //    val phase: Phase[Event] = ((e: Event) => e.speed) > 4

    val phase2: Phase[Event] = avg('speed.field[Event], window) > avg('pump.field, window)

    val phase3 = avg('speed.field, 5.seconds) >= 5.0 andThen avg('pump.field, 3.seconds) > 0

    val phase4: Phase[Event] = avg('speed.field, 5.seconds) >= value(5.0)

    val phase5: Phase[Event] = ('speed.field > 4 & 'pump.field > 100).timed(more(10.seconds))

    val t: Phase[Event] = 'speed.field >= 100

    val decr: Phase[Event] = ('speed.field === 100) andThen (avg(Derivation('speed.field), 3.seconds) < 0) until ('speed.field <= 50)

    val phase6 = 'currentCompressorMotor.field > 0 togetherWith
      ('PAirMainRes.field <= 7.5 andThen (Derivation(avg('PAirMainRes.field, 5.seconds)) > 0).timed(more(23.seconds))
        .until('PAirMainRes.field >= 8.0))

    val phase7: Phase[Appevent] =
      ('eventType.as[String] === "TableJoin")
        .andThen(
          not(
            ('eventType.as[String] === "Bet_ACCEPTED")
              or
              ('eventType.as[String] === "Bet2")
          ).timed(more(30.seconds))
        )
  }


}

case class Event(speed: Int, time: Instant)

