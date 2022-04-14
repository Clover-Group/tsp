package ru.itclover.tsp.serializers

import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.types.Row
import ru.itclover.tsp.io.output.EventSchema
import ru.itclover.tsp.serializers.core.JSONSerialization

object KafkaSerializers {

  class JSONSerializer(rowSchema: EventSchema) extends SerializationSchema[Row] {
    override def serialize(element: Row): Array[Byte] = new JSONSerialization().serialize(element, rowSchema)
  }

}
