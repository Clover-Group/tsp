package ru.itclover.tsp.core.utils

import java.time.Instant

import cats.Id
import org.openjdk.jol.info.GraphLayout
import ru.itclover.tsp.core.fixtures.Event
import ru.itclover.tsp.core.utils.TimeSeriesGenerator.Increment
import ru.itclover.tsp.core.{Pattern, StateMachine}

object PatternMemoryCheck {

  private def runAndReturnFinalState[A, S](pattern: Pattern[A, S, _], events: Seq[A], groupSize: Int) = {
    StateMachine[Id].run(pattern, events, pattern.initialState(), groupSize = groupSize)
  }

  private def generateSeq(generator: TimeSeriesGenerator[Int], amount: Int) = {
    (for (time <- Timer(from = Instant.now());
          idx  <- Increment;
          row  <- generator)
      yield Event[Int](time.toEpochMilli, idx.toLong, row.toInt, 0)).run(seconds = amount)
  }

  def finalStateSize[A, S <: AnyRef](pattern: Pattern[A, S, _], events: Seq[A], groupSize: Int): Long = {
    val finalState = runAndReturnFinalState(pattern, events, groupSize)
    val layout = GraphLayout.parseInstance(finalState)
    layout.totalSize()
  }

  def finalStateSize[A, S <: AnyRef](pattern: Pattern[A, S, _], event: A, amount: Int, groupSize: Int): Long = {
    val finalState = runAndReturnFinalState(pattern, Seq.tabulate(amount)(_ => event), groupSize)
    val layout = GraphLayout.parseInstance(finalState)
    layout.totalSize()
  }

  /**
    * Returns final size (in bytes) of state after running StateMachine with pattern on event.s
    */
  def finalStateSizeGenerator[S <: AnyRef](
    pattern: Pattern[Event[Int], S, _],
    generator: TimeSeriesGenerator[Int],
    amount: Int,
    groupSize: Int = 1000
  ): Long = {
    val finalState = runAndReturnFinalState(pattern, generateSeq(generator, amount), groupSize)
    val layout = GraphLayout.parseInstance(finalState)
    layout.totalSize()
  }

}
