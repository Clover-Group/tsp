package ru.itclover.tsp.transformers

import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.types.Row
import ru.itclover.tsp.io.output.RedisOutputConf
import ru.itclover.tsp.services.RedisService

import org.redisson.client.codec.ByteArrayCodec

/**
  * Sink function impl for Redis sink
  * @param conf redis output conf
  */
class RedisSinkFunction[ITEM](conf: RedisOutputConf) extends RichSinkFunction[Row] {

  /**
    * Method for sink impl
    * @param value value for save
    * @param context sink context
    */
  override def invoke(value: Row, context: SinkFunction.Context): Unit = {

    val redisInfo = RedisService.clientInstance(this.conf, conf.serializer)
    val client = redisInfo._1
    val serializer = redisInfo._2

    val bucket = client.getBucket[Array[Byte]](conf.key, ByteArrayCodec.INSTANCE)
    bucket.set(serializer.serialize(value, conf.rowSchema))

  }

}
