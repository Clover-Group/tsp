package ru.itclover.tsp.http.domain.input
import ru.itclover.tsp.core.RawPattern
import ru.itclover.tsp.io.input.InputConf
import ru.itclover.tsp.io.output.OutputConf

trait Request


final case class FindPatternsRequest[IN <: InputConf[_, _, _], OUT <: OutputConf[_]](
  uuid: String, inputConf: IN, outConf: OUT, patterns: Seq[RawPattern]
) extends Request

final case class DSLPatternRequest(pattern: String) extends Request
