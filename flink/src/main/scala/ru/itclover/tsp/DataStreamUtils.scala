package ru.itclover.tsp

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.typeutils.UnitTypeInfo
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream}
import org.apache.flink.types.Row
import ru.itclover.tsp.transformers.{FlatMappersCombinator, RichStatefulFlatMapper}


object DataStreamUtils {

  implicit class DataStreamOps[Event: TypeInformation](val stream: DataStream[Event]) {

    def flatMapIf(cond: Boolean, mapper: => FlatMapFunction[Event, Event]): DataStream[Event] = {
      if (cond) { stream.flatMap(mapper) } else { stream }
    }

    def foreach(fn: Event => Unit): DataStream[Unit] = stream.map[Unit](fn)(new UnitTypeInfo)

  }
}
