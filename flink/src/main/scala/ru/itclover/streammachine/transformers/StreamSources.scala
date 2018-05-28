package ru.itclover.streammachine.transformers

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import ru.itclover.streammachine.io.input.{InputConf, JDBCInputConf, RawPattern}
import ru.itclover.streammachine.http.utils.ImplicitUtils.RightBiasedEither
import org.apache.flink.streaming.api.scala._


object StreamSources {

  def fromJdbc(inputConf: JDBCInputConf)
              (implicit streamEnv: StreamExecutionEnvironment) = inputConf.fieldsTypesInfo map { fTypesInfo =>
    streamEnv.createInput(inputConf.getInputFormat(fTypesInfo.toArray)).name("Input processing stage")
  }

  // def fromInfluxDB(inputConf: InputConf)

}
