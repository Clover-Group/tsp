package ru.itclover.tsp.utils

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala.DataStream
import ru.itclover.tsp.DebugTsViolationHandler

object DataStreamOps {

  implicit class DataStreamOps[E: TypeInformation](val stream: DataStream[E]) {

//    def flatMapIf(cond: Boolean, mapper: => FlatMapFunction[E, E]): DataStream[E] = {
//      if (cond) { stream.flatMap(mapper) } else { stream }
//    }

//    def foreach(fn: E => Unit): DataStream[Unit] = stream.map[Unit](fn)(new UnitTypeInfo) // It seems to be unneeded

    def assignAscendingTimestamps_withoutWarns(extractor: E => Long): DataStream[E] = {
      // Careful! Closure cleaner is disabled bcs it's private for Flink for some stupid reason // val cleanExtractor = stream.clean(extractor)
      val extractorFunction = new AscendingTimestampExtractor[E] {
        def extractAscendingTimestamp(element: E): Long =
          extractor(element)
      }.withViolationHandler(new DebugTsViolationHandler())
      stream.assignTimestampsAndWatermarks(extractorFunction)
    }

  }
}
