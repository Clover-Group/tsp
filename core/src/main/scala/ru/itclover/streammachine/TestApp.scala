package ru.itclover.streammachine

import java.time.Instant

import ru.itclover.streammachine.core.Aggregators.Timer
import ru.itclover.streammachine.phases.Phases.{Assert, Decreasing}
import ru.itclover.streammachine.core.TimeImplicits._

object TestApp extends App {

  val now = Instant.now()
  val timeSeq = Stream.from(0).map(t => now.plusSeconds(t.toLong))

  val events = 100.to(0, -1).zip(timeSeq).map(Event.tupled)

  val phase =
    Decreasing[Event, Int](_.speed, 250, 50)
      .andThen(
        Assert[Event](_.speed > 0)
          and
          Timer[Event, Instant](_.time, 10, 30)
      )

  val collector = events.foldLeft(StateMachineMapper(phase)) { case (mapper, event) => mapper(event) }

//  Average[Event](5 seconds, _.speed, 'avgSpeed)

  collector.result.foreach(println)
}

case class Event(speed: Int, time: Instant)

