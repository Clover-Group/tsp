package ru.itclover.tsp.transformers

import com.typesafe.scalalogging.Logger
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.types.Row
import ru.itclover.tsp.io.input.SerializerInfo
import ru.itclover.tsp.io.output.RedisOutputConf
import ru.itclover.tsp.services.RedisService
import scredis.serialization.Reader
import java.time.LocalDateTime

/**
  * Sink function impl for Redis sink
  * @param conf redis output conf
  * @param info serializer info
  */
class RedisSinkFunction[ITEM](conf: RedisOutputConf, info: SerializerInfo) extends RichSinkFunction[Row] {

  /**
    * Method for sink impl
    * @param value value for save
    * @param context sink context
    */
  override def invoke(value: Row, context: SinkFunction.Context[_]): Unit = {

    val logger = Logger[RedisSinkFunction[Row]]

    val (client, serializer) = RedisService.clientInstance(this.conf, this.info)

    implicit val reader: Reader[Array[Byte]] = (bytes: Array[Byte]) => bytes

    val resultKey = s"${info.key}_${LocalDateTime.now().toString}"
    logger.info(s"Result key for ${value} : $resultKey")

    client.set[Array[Byte]](resultKey, serializer.serialize(value, conf.rowSchema))
    client.quit().value.get.get

  }

}
