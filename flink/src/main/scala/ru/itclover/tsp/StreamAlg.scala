/*
package ru.itclover.tsp

import scala.language.higherKinds
import cats.Semigroup
import cats.implicits._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SessionWindowTimeGapExtractor}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import ru.itclover.tsp.core.Incident
import ru.itclover.tsp.io.TimeExtractor
import ru.itclover.tsp.io.output.OutputConf
import ru.itclover.tsp.mappers.{FlatMappersCombinator, StatefulFlatMapper}
import scala.reflect.ClassTag

trait StreamAlg[S[_], KeyedS[_, _] <: S, TypeInfo[_]] {

  def createStream[In, Key, Item](source: StreamSource[In, Key, Item]): S[In]

  def keyBy[In, K: TypeInfo](stream: S[In], partitioner: In => K, maxPartitions: Int): KeyedS[In, K]

  def map[In, Out: TypeInfo, State](stream: S[In])(f: In => Out): S[Out]

  def flatMapWithState[In, State: ClassTag, Out: TypeInfo, K](stream: KeyedS[In, K])(
    mappers: Seq[StatefulFlatMapper[In, State, Out]],
    initState: State
  ): S[Out]

  def reduceNearby[In: Semigroup: TimeExtractor, K](stream: KeyedS[In, K], getSessionSize: In => Long): S[In]

  def addSink[T](stream: S[T], outputConf: OutputConf[T]): S[T]
}

// .. todo
case class FlinkStreamAlg() extends StreamAlg[DataStream, KeyedStream, TypeInformation] {

  override def createStream[In, Key, Item](source: StreamSource[In, Key, Item]) =
    source
      .createStream
      .assignAscendingTimestamps(p => source.timeExtractor.apply(p).toMillis)

  override def keyBy[In, K: TypeInformation](stream: DataStream[In], partitioner: In => K, maxPartitions: Int): KeyedStream[In, K] =
    stream
      .setMaxParallelism(maxPartitions)
      .keyBy(partitioner)

  override def flatMapWithState[In, State: ClassTag, Out: TypeInformation, K](stream: KeyedStream[In, K])(
    mappers: Seq[StatefulFlatMapper[In, State, Out]],
    initState: State
  ) =
    stream.flatMap(new FlatMappersCombinator(mappers, initState))

  override def map[In, Out: TypeInformation, State](stream: DataStream[In])(f: In => Out)  =
    stream.map(f)

  override def reduceNearby[In: Semigroup: TimeExtractor, K](
    stream: KeyedStream[In, K],
    getSessionSize: In => Long
  ): DataStream[In] =
    stream
      .window(EventTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor[In] {
        override def extract(element: In): Long = getSessionSize(element)
      }))
      .reduce { _ |+| _ }
      .name("Uniting adjacent items")

  override def addSink[T](stream: DataStream[T], outputConf: OutputConf[T]): DataStream[T] = {
    stream.writeUsingOutputFormat(outputConf.getOutputFormat)
    stream
  }
}
*/
