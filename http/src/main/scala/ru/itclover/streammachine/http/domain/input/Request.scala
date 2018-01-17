package ru.itclover.streammachine.http.domain.input

import ru.itclover.streammachine.io.input.{ClickhouseInput, JDBCInputConfig => InputConfigs}
import ru.itclover.streammachine.io.output.{JDBCOutputConfig => OutputConfigs}

trait Request


final case class IORequest(source: InputConfigs, sink: OutputConfigs) extends Request
