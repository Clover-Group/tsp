package ru.itclover.streammachine.http.domain.input

import ru.itclover.streammachine.io.input.{InputConf, JDBCInputConf, JDBCNarrowInputConf, RawPattern}
import ru.itclover.streammachine.io.output.{JDBCOutputConf, OutputConf}

trait Request


final case class FindPatternsRequest[IN <: InputConf, OUT <: OutputConf]
(source: IN, sink: OUT, patterns: Seq[RawPattern])
  extends Request
