package ru.itclover.tsp.streaming.checkpointing

import ru.itclover.tsp.core.{RawPattern, Segment}
import ru.itclover.tsp.core.optimizations.Optimizer.{S => State}
import scala.util.Try

case class Checkpoint(readRows: Long = 0, writtenRows: List[Long] = List.empty) extends Serializable {
  // not good, but def does not work for some reasons
  val totalWrittenRows: Long = writtenRows.foldLeft(0L)(_ + _)
}

case class CheckpointState(states: Map[RawPattern, State[Segment]] = Map.empty) extends Serializable
