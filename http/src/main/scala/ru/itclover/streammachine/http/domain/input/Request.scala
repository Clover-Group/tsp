package ru.itclover.streammachine.http.domain.input

import ru.itclover.streammachine.io.input.{JDBCInputConfig => InputConfigs}
import ru.itclover.streammachine.io.output.{JDBCOutputConfig => OutputConfigs}

trait Request


final case class FindPatternsRequest(source: InputConfigs, sink: OutputConfigs, patternsIdsAndCodes: Map[String, String])
  extends Request
