package ru.itclover.streammachine.http.domain.input

import ru.itclover.streammachine.io.input.{InputConf, RawPattern}
import ru.itclover.streammachine.io.output.OutputConf

trait Request


final case class FindPatternsRequest[IN <: InputConf[_], OUT <: OutputConf[_]](
  uuid: String, source: IN, sink: OUT, patterns: Seq[RawPattern]
) extends Request

final case class DSLPatternRequest(pattern: String) extends Request
