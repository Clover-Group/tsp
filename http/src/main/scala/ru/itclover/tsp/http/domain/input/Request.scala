package ru.itclover.tsp.http.domain.input
import ru.itclover.tsp.core.RawPattern

trait Request

final case class FindPatternsRequest[IN/* <: InputConf[_, _, _]*/, OUT /*<: OutputConf[_]*/](
  uuid: String,
  inputConf: IN,
  outConf: OUT,
  patterns: Seq[RawPattern]
) extends Request

final case class DSLPatternRequest(pattern: String) extends Request
