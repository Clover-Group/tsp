package ru.itclover.tsp.http.domain.input

import ru.itclover.tsp.core.RawPattern
import ru.itclover.tsp.streaming.io.{InputConf, OutputConf}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

trait Request

sealed trait QueueableRequest extends Request with Ordered[QueueableRequest] with Serializable {
  def priority: Int
  def uuid: String

  override def compare(that: QueueableRequest): Int = this.priority.compare(that.priority)

  def requiredSlots: Int

  val maxLength = 1000000

  def serialize: Array[Byte] = {
    val baos = new ByteArrayOutputStream(maxLength)
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(this)
    oos.close
    baos.toByteArray
  }

}

object QueueableRequest {

  def deserialize(data: Array[Byte]): QueueableRequest = {
    val ois = new ObjectInputStream(new ByteArrayInputStream(data))
    ois.readObject().asInstanceOf[QueueableRequest]
  }

}

final case class FindPatternsRequest[Event, EKey, EItem, OutEvent](
  override val uuid: String,
  inputConf: InputConf[Event, EKey, EItem],
  outConf: Seq[OutputConf[OutEvent]],
  override val priority: Int,
  patterns: Seq[RawPattern]
) extends QueueableRequest {

  override def requiredSlots: Int =
    (inputConf.parallelism.getOrElse(1)
      * inputConf.patternsParallelism.getOrElse(1)
      * inputConf.numParallelSources.getOrElse(1))

}

final case class DSLPatternRequest(pattern: String) extends Request
