package ru.itclover.streammachine

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.typeutils.UnitTypeInfo
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.types.Row
import ru.itclover.streammachine.core.Time.TimeExtractor
import ru.itclover.streammachine.io.input.JDBCNarrowInputConf
import ru.itclover.streammachine.io.input.source.JDBCSourceInfo
import ru.itclover.streammachine.io.output._
import ru.itclover.streammachine.transformers.{FlatMappersCombinator, SparseRowsDataAccumulator}
import sun.reflect.generics.reflectiveObjects.NotImplementedException


object DataStreamUtils {

  implicit class DataStreamOps[Event](val dataStream: DataStream[Event]) {

    def accumulateKeyValues(sourceInfo: JDBCSourceInfo, inputConf: JDBCNarrowInputConf)
                           (implicit timeExtractor: TimeExtractor[Event],
                                extractKeyVal: Event => (Symbol, Double),
                                extractAny: (Event, Symbol) => Any,
                                rowTypeInfo: TypeInformation[Row]): DataStream[Row] = {
      val extraFields = sourceInfo.fieldsIndexesMap.filterNot(nameAndInd =>
        nameAndInd._1 == inputConf.keyColname || nameAndInd._1 == inputConf.valColname
      ).keys.toSeq
      val dataAccumulator = SparseRowsDataAccumulator(inputConf.fieldsTimeoutsMs, extraFields)
                                                     (timeExtractor, extractKeyVal, extractAny)
      dataStream.flatMap(dataAccumulator)
    }

    def flatMapAll[Out: TypeInformation](flatMappers: Iterable[RichFlatMapFunction[Event, Out]]): DataStream[Out] =
      dataStream.flatMap(FlatMappersCombinator(flatMappers))

    def foreach(fn: Event => Unit): DataStream[Unit] = dataStream.map[Unit](fn)(new UnitTypeInfo)

  }
}
