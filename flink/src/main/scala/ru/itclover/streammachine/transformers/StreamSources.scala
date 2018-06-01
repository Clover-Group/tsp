package ru.itclover.streammachine.transformers

import java.util
import collection.JavaConversions._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import ru.itclover.streammachine.io.input.{InfluxDBInputConf, InputConf, JDBCInputConf, RawPattern}
import ru.itclover.streammachine.http.utils.ImplicitUtils.RightBiasedEither
import org.apache.flink.streaming.api.scala._
import org.apache.flink.types.Row


object StreamSources {

  def fromJdbc(inputConf: JDBCInputConf)
              (implicit streamEnv: StreamExecutionEnvironment) = inputConf.fieldsTypesInfo map { fTypesInfo =>
    streamEnv.createInput(inputConf.getInputFormat(fTypesInfo.toArray)).name("JDBC input processing stage")
  }

  def fromInfluxDB(inputConf: InfluxDBInputConf)
                (implicit streamEnv: StreamExecutionEnvironment): DataStream[Row] = {
    streamEnv.createInput(inputConf.getInputFormat)(inputConf.resultTypeInfo)
      .flatMap(queryResult => for {  // extract Flink.rows form series of points
        series <- queryResult.getSeries
        valueSet <- series.getValues
      } yield {
        val tags = if (series.getTags != null) series.getTags else new util.HashMap[String, String]()
        val row = new Row(tags.size() + valueSet.size())
        tags.toSeq.sortBy(_._1).zipWithIndex.foreach { case ((_, tag), ind) => row.setField(ind, tag) }
        valueSet.zipWithIndex.foreach { case (value, ind) => row.setField(ind, value) }
        row
      })
      .name("InfluxDB input processing stage")
  }
}
