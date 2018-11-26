package ru.itclover.tsp

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.typeutils.UnitTypeInfo
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.types.Row
import ru.itclover.tsp.transformers.{FlatMappersCombinator, RichStatefulFlatMapper}


object DataStreamUtils {

  implicit class DataStreamOps[Event: TypeInformation](val dataStream: DataStream[Event]) {

    /*def accumulateKeyValues(sourceInfo: JDBCSourceInfo, inputConf: JDBCNarrowInputConf)
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
    }*/

    def flatMapAll[Out: TypeInformation](flatMappers: Seq[RichStatefulFlatMapper[Event, Any, Out]]): DataStream[Out] = {
      dataStream.flatMap(new FlatMappersCombinator[Event, Any, Out](flatMappers))
    }

    def flatMapIf(cond: Boolean, mapper: => FlatMapFunction[Event, Event]): DataStream[Event] = {
      if (cond) { dataStream.flatMap(mapper) } else { dataStream }
    }

    def foreach(fn: Event => Unit): DataStream[Unit] = dataStream.map[Unit](fn)(new UnitTypeInfo)

  }
}
