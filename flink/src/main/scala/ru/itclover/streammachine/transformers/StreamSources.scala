package ru.itclover.streammachine.transformers

import java.util
import collection.JavaConversions._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import ru.itclover.streammachine.io.input.{InfluxDBInputConf, InputConf, JDBCInputConf, RawPattern}
import ru.itclover.streammachine.utils.CollectionsOps.RightBiasedEither
import org.apache.flink.streaming.api.scala._
import org.apache.flink.types.Row
import scala.collection.mutable


object StreamSources {

  def fromJdbc(inputConf: JDBCInputConf)
              (implicit streamEnv: StreamExecutionEnvironment) = inputConf.fieldsTypesInfo map { fTypesInfo =>
    val stream = streamEnv
      .createInput(inputConf.getInputFormat(fTypesInfo.toArray))
      .name("JDBC input processing stage")
    inputConf.parallelism match {
      case Some(p) => stream.setParallelism(p)
      case None => stream
    }
  }

  def fromInfluxDB(inputConf: InfluxDBInputConf)
                (implicit streamEnv: StreamExecutionEnvironment): DataStream[Row] = {
    streamEnv.createInput(inputConf.getInputFormat)(inputConf.resultTypeInfo)
      .flatMap(queryResult => {
        // extract Flink.rows form series of points
        if (queryResult == null || queryResult.getSeries == null) {
          mutable.Buffer[Row]()
        }
        else for {
          series <- queryResult.getSeries
          valueSet <- series.getValues
        } yield {
          val tags = if (series.getTags != null) series.getTags else new util.HashMap[String, String]()
          val row = new Row(tags.size() + valueSet.size())
          tags.toSeq.sortBy(_._1).zipWithIndex.foreach { case ((_, tag), ind) => row.setField(ind, tag) }
          valueSet.zipWithIndex.foreach { case (value, ind) => row.setField(ind, value) }
          row
        }
      })
      .name("InfluxDB input processing stage")
  }
}
