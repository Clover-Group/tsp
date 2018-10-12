package ru.itclover.tsp.http.domain.input

import ru.itclover.tsp.dsl.schema.RawPattern
import ru.itclover.tsp.io.input.InputConf
import ru.itclover.tsp.io.output.OutputConf

trait Request


final case class FindPatternsRequest[IN <: InputConf[_], OUT <: OutputConf[_]](
  uuid: String, source: IN, sink: OUT, patterns: Seq[RawPattern]
) extends Request

final case class DSLPatternRequest(pattern: String) extends Request
