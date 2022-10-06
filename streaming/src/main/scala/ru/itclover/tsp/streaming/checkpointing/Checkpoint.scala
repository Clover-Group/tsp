package ru.itclover.tsp.streaming.checkpointing

import ru.itclover.tsp.core.{RawPattern, Segment}
import ru.itclover.tsp.core.optimizations.Optimizer.{S => State}

case class Checkpoint(readRows: Long = 0, writtenRows: Long = 0) extends Serializable

case class CheckpointState(states: Map[RawPattern, State[Segment]] = Map.empty) extends Serializable
