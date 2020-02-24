package ru.itclover.tsp.serializers

import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.types.Row
import ru.itclover.tsp.io.output.RowSchema
import ru.itclover.tsp.serializers.core.{ArrowSerialization, JSONSerialization, ParquetSerialization}

object KafkaSerializers {

  class JSONSerializer(rowSchema: RowSchema) extends SerializationSchema[Row]{
    override def serialize(element: Row): Array[Byte] = new JSONSerialization().serialize(element, rowSchema)
  }

  class ArrowSerializer(rowSchema: RowSchema) extends SerializationSchema[Row]{
    override def serialize(element: Row): Array[Byte] = new ArrowSerialization().serialize(element, rowSchema)
  }

  class ParquetSerializer(rowSchema: RowSchema) extends SerializationSchema[Row]{
    override def serialize(element: Row): Array[Byte] = new ParquetSerialization().serialize(element, rowSchema)
  }

}
