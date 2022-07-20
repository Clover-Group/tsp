package ru.itclover.tsp.streaming.checkpointing

import com.typesafe.scalalogging.Logger
import org.redisson.Redisson
import org.redisson.codec.Kryo5Codec
import org.redisson.config.Config
import ru.itclover.tsp.StreamSource.Row
import ru.itclover.tsp.core.{Pattern, RawPattern, Segment}
import ru.itclover.tsp.core.optimizations.Optimizer.{S => State}

import scala.collection.mutable
import scala.util.Try

case class CheckpointingService(redisUri: String) {
  //val codec = new Kryo5Codec()

  val redisConfig = new Config()//.setCodec(codec)

  val log = Logger("Checkpointing")

  redisConfig
    .useSingleServer()
    .setConnectionMinimumIdleSize(16)
    .setConnectionPoolSize(32)
    .setAddress(redisUri)

  val redissonClient = Redisson.create(redisConfig)

  def updateCheckpointRead(uuid: String,
                           newRowsRead: Long,
                           newStates: Map[RawPattern, State[Segment]]): Unit = {
    val checkpointBucket = redissonClient.getBucket[Checkpoint](s"tsp-cp-$uuid")
    val checkpoint = Option(checkpointBucket.get).getOrElse(Checkpoint(0, 0))
    val newCheckpoint = checkpoint.copy(
      readRows = checkpoint.readRows + newRowsRead
    )
    val checkpointStateBucket = redissonClient.getBucket[CheckpointState](s"tsp-cp-$uuid-state")
    log.warn(s"Checkpointing $uuid: ${checkpoint.readRows + newRowsRead} rows read")
    checkpointBucket.set(newCheckpoint)
    checkpointStateBucket.set(CheckpointState(newStates))
  }

  def updateCheckpointWritten(uuid: String,
                           newRowsWritten: Long): Unit = {
    val checkpointBucket = redissonClient.getBucket[Checkpoint](s"tsp-cp-$uuid")
    val checkpoint = checkpointBucket.get()
    val newCheckpoint = checkpoint.copy(
      writtenRows = checkpoint.writtenRows + newRowsWritten,
    )
    checkpointBucket.set(newCheckpoint)
  }

  def getCheckpointAndState(uuid: String): (Option[Checkpoint], Option[CheckpointState]) = {
    val checkpointBucket = redissonClient.getBucket[Checkpoint](s"tsp-cp-$uuid")
    val checkpointStateBucket = redissonClient.getBucket[CheckpointState](s"tsp-cp-$uuid-state")
    (Option(checkpointBucket.get()), Option(checkpointStateBucket.get()))
  }
}

object CheckpointingService {
  private var service: Option[CheckpointingService] = None

  def getOrCreate(redisUri: String): CheckpointingService = service match {
    case Some(value) =>
      value
    case None =>
      val srv = CheckpointingService(redisUri)
      service = Some(srv)
      srv
  }

  def updateCheckpointRead(uuid: String,
                           newRowsRead: Long,
                           newStates: Map[RawPattern, State[Segment]]): Unit =
    service.map(_.updateCheckpointRead(uuid, newRowsRead, newStates)).getOrElse(())

  def updateCheckpointWritten(uuid: String,
                              newRowsWritten: Long): Unit =
    service.map(_.updateCheckpointWritten(uuid, newRowsWritten)).getOrElse(())

  def getCheckpointAndState(uuid: String): (Option[Checkpoint], Option[CheckpointState]) =
    service.map(_.getCheckpointAndState(uuid)).getOrElse((None, None))
}