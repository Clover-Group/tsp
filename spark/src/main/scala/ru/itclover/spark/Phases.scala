package ru.itclover.spark

import ru.itclover.streammachine.core.PhaseParser.Functions._
import ru.itclover.streammachine.core.PhaseParser._
import ru.itclover.streammachine.core.Time
import ru.itclover.streammachine.core.Time.{TimeExtractor, _}
import ru.itclover.streammachine.phases.BooleanPhases.Assert
import ru.itclover.streammachine.phases.NumericPhases.{SymbolExtractor, SymbolNumberExtractor, _}
import ru.itclover.streammachine.phases.Phases.Phase
import ru.itclover.streammachine.phases.TimePhases.Wait

import scala.concurrent.duration._

object Phases {

  implicit val symbolExtractorAppevent: SymbolExtractor[Appevent, String] = new SymbolExtractor[Appevent, String] {
    override def extract(event: Appevent, symbol: Symbol): String = {
      symbol match {
        case 'eventType => event.eventType
        case 'userId => event.userId
        case _ => sys.error(s"No field $symbol in $event")
      }
    }
  }

  implicit val symbolNumberExtractorAppevent: SymbolNumberExtractor[Appevent] = new SymbolNumberExtractor[Appevent] {
    override def extract(event: Appevent, symbol: Symbol) = symbol match {
      case 'created => event.created
    }
  }

  implicit val extractTimeAppevent: TimeExtractor[Appevent] = new TimeExtractor[Appevent] {
    override def apply(v1: Appevent): Time = v1.created
  }

  val fastLeaversRule =
    Assert('eventType.as[String] === "TABLE_JOIN")
      .andThen(
        Wait('eventType.as[String] === "TABLE_LEAVE").timed(less(30.seconds))
      ).mapWithEvent {
      case (event, (_, (_, (start, end)))) => Tuple3(event.userId, start.toMillis, end.toMillis)
    }

  val vipUsersRule =
    Assert(
      'userId.as[String] in Set("user1", "mghc2b2vycnqacms")
        and
        'eventType.as[String] === "TABLE_JOIN"
    ).mapWithEvent { case (e, _) => e.userId -> e.created }

  val simpleParser = avg('created.field, 3.seconds)

}
