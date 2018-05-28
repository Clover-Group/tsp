package ru.itclover.streammachine

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.typeutils.UnitTypeInfo
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.types.Row
import ru.itclover.streammachine.core.Time.TimeExtractor
import ru.itclover.streammachine.transformers.{FlatMappersCombinator, FlinkPatternMapper, SparseRowsDataAccumulator, RichStatefulFlatMapper}
import sun.reflect.generics.reflectiveObjects.NotImplementedException


object DataStreamUtils {

  implicit class DataStreamOps[Event](val dataStream: DataStream[Event]) {

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

    def foreach(fn: Event => Unit): DataStream[Unit] = dataStream.map[Unit](fn)(new UnitTypeInfo)

  }
}
