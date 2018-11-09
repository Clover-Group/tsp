package ru.itclover.tsp

import java.time.Instant
import ru.itclover.tsp.aggregators.AggregatorPhases.Derivation
import ru.itclover.tsp.core.Time._
import ru.itclover.tsp.core._
import ru.itclover.tsp.core.Pattern.Functions._
import ru.itclover.tsp.phases.NumericPhases.SymbolParser
import ru.itclover.tsp.phases.NumericPhases._
import ru.itclover.tsp.phases.Phases.Decreasing
import scala.concurrent.duration._


object TestApp extends App {

  case class TestEvent(speed: Int, time: Instant)

  implicit val extractTime: TimeExtractor[TestEvent] = new TimeExtractor[TestEvent] {
    override def apply(v1: TestEvent) = v1.time
  }

  def run[T, Out](rule: Pattern[T, _, Out], events: Seq[T]) = {
    val mapResults = FakeMapper[T, Out]()
    events
      .foldLeft(PatternMapper(rule, mapResults)) { case (machine, event) => machine(event) }
      .result
  }

  val now = Instant.now()
  val timeSeq = Stream.from(0).map(t => now.plusSeconds(t.toLong))

  val events = 100.to(0, -1).zip(timeSeq).map(TestEvent.tupled)

  val phase =
    Decreasing[TestEvent, Int](_.speed, 250, 50)
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

    implicit val symbolNumberExtractorEvent: SymbolNumberExtractor[TestEvent] = new SymbolNumberExtractor[TestEvent] {
      override def extract(event: TestEvent, symbol: Symbol) = {
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

    type Phase[Event] = Pattern[Event, _, _]

    val phase2: Phase[TestEvent] = avg('speed.asDouble, window) > avg('pump.asDouble, window)

    val phase3 = avg('speed.asDouble, 5.seconds) >= 5.0 andThen avg('pump.asDouble, 3.seconds) > 0

    val phase4: Phase[TestEvent] = avg('speed.asDouble, 5.seconds) >= value(5.0)

    val phase5: Phase[TestEvent] = ('speed.asDouble > 4 & 'pump.asDouble > 100).timed(more(10.seconds))

    val t: Phase[TestEvent] = 'speed.asDouble >= 100

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

