package ru.itclover.tsp.http.domain.input
import ru.itclover.tsp.core.RawPattern

trait Request

trait QueueableRequest extends Request with Ordered[QueueableRequest] {
  def priority: Int
  def uuid: String

  override def compare(that: QueueableRequest): Int = this.priority.compare(that.priority)
}

final case class FindPatternsRequest[IN /* <: InputConf[_, _, _]*/, OUT /*<: OutputConf[_]*/ ] (
  override val uuid: String,
  inputConf: IN,
  outConf: OUT,
  override val priority: Int,
  patterns: Seq[RawPattern]
) extends QueueableRequest

final case class DSLPatternRequest(pattern: String) extends Request
