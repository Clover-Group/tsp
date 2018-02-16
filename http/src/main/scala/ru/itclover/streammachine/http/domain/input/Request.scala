package ru.itclover.streammachine.http.domain.input

import ru.itclover.streammachine.io.input.{InputConf, JDBCInputConf, JDBCNarrowInputConf}
import ru.itclover.streammachine.io.output.{JDBCOutputConf, OutputConf}

trait Request


final case class FindPatternsRequest[IN <: InputConf, OUT <: OutputConf]
(source: IN, sink: OUT, patternsIdsAndCodes: Map[String, String])
  extends Request
