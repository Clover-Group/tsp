package ru.itclover.streammachine

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.types.Row
import ru.itclover.streammachine.io.input.source.JDBCSourceInfo
import ru.itclover.streammachine.io.output._
import ru.itclover.streammachine.transformers.FlatMappersCombinator
import sun.reflect.generics.reflectiveObjects.NotImplementedException


object DataStreamUtils {

  implicit class DataStreamOps[T](val dataStream: DataStream[T]) {
    def condenseNarrowKeyValues(stream: DataStream[Row], sourceInfo: JDBCSourceInfo): DataStream[Row] = {
      // TODO(1): DataAccumulator[SourceInfo, SinkInfo] and (0, 1), Seq(1 -> 'a) params
      throw new NotImplementedException()
      /*val defaultTimeouts = Seq.fill(fields.length)(defaultFieldTimeout)
    val dataAccumulator = SparseRowsDataAccumulator(fields.zip(defaultTimeouts).toMap, (0, 1), Seq(1 -> 'a))(timeExtractor)
    stream.flatMap(dataAccumulator)*/
    }

    def flatMapAll[Out: TypeInformation](flatMappers: Iterable[RichFlatMapFunction[T, Out]]) =
      dataStream.flatMap(FlatMappersCombinator(flatMappers))
  }
}
