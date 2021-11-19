package ru.itclover.tsp.http.domain.input
import ru.itclover.tsp.core.RawPattern
import ru.itclover.tsp.io.input.InputConf
import ru.itclover.tsp.io.output.OutputConf

trait Request

trait QueueableRequest extends Request with Ordered[QueueableRequest] {
  def priority: Int
  def uuid: String

  override def compare(that: QueueableRequest): Int = this.priority.compare(that.priority)

  def requiredSlots: Int
}

final case class FindPatternsRequest[IN <: InputConf[_, _, _], OUT <: OutputConf[_]] (
  override val uuid: String,
  inputConf: IN,
  outConf: OUT,
  override val priority: Int,
  patterns: Seq[RawPattern]
) extends QueueableRequest {
  override def requiredSlots: Int =
    (inputConf.parallelism.getOrElse(1)
      * inputConf.patternsParallelism.getOrElse(1)
      * inputConf.numParallelSources.getOrElse(1))
}

final case class DSLPatternRequest(pattern: String) extends Request
