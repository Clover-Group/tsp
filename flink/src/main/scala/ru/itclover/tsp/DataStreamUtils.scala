package ru.itclover.tsp

import org.apache.flink.api.common.functions.{FlatMapFunction, RichFlatMapFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.typeutils.UnitTypeInfo
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.types.Row
import ru.itclover.tsp.core.Time.TimeExtractor
import ru.itclover.tsp.transformers.{FlatMappersCombinator, FlinkCompilingPatternMapper, RichStatefulFlatMapper, SparseRowsDataAccumulator}
import sun.reflect.generics.reflectiveObjects.NotImplementedException


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
      lazy val m = mapper // don't create mapper if the condition is false
      if (cond) { dataStream.flatMap(m) } else { dataStream }
    }

    def foreach(fn: Event => Unit): DataStream[Unit] = dataStream.map[Unit](fn)(new UnitTypeInfo)

  }
}
