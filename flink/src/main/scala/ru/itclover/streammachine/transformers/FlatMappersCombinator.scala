package ru.itclover.streammachine.transformers

import org.apache.flink.api.common.functions.{RichFlatMapFunction, RuntimeContext}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector


case class FlatMappersCombinator[IN, OUT](flatMappers: Iterable[RichFlatMapFunction[IN, OUT]])
    extends RichFlatMapFunction[IN, OUT] {
  override def open(config: Configuration): Unit = {
    super.open(config)
    flatMappers.foreach(_.open(config))
  }

  override def flatMap(value: IN, out: Collector[OUT]): Unit = flatMappers.foreach(_.flatMap(value, out))

  override def setRuntimeContext(ctx: RuntimeContext): Unit = {
    super.setRuntimeContext(ctx)
    flatMappers.foreach(_.setRuntimeContext(ctx))
  }

  override def close(): Unit = {
    flatMappers.foreach(_.close())
    super.close()
  }
}
