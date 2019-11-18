package ru.itclover.tsp.serializers

import java.io.ByteArrayOutputStream

import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.types.Row
import org.codehaus.jackson.map.ObjectMapper
import ru.itclover.tsp.io.output.RowSchema

/**
* Class for serializing Flink row to JSON
  * @author trolley813
  * @param rowSchema schema of row
  */
class JSONSerializer(rowSchema: RowSchema) extends SerializationSchema[Row]{
  override def serialize(element: Row): Array[Byte] = {

    val out = new ByteArrayOutputStream
    val mapper = new ObjectMapper()
    val root = mapper.createObjectNode()
    root.put(rowSchema.sourceIdField.name, element.getField(rowSchema.sourceIdInd).asInstanceOf[Int])
    root.put(rowSchema.fromTsField.name, element.getField(rowSchema.beginInd).asInstanceOf[Double])
    root.put(rowSchema.toTsField.name, element.getField(rowSchema.endInd).asInstanceOf[Double])
    root.put(rowSchema.appIdFieldVal._1.name, element.getField(rowSchema.appIdInd).asInstanceOf[Int])
    root.put(rowSchema.patternIdField.name, element.getField(rowSchema.patternIdInd).asInstanceOf[String])
    root.put(rowSchema.processingTsField.name, element.getField(rowSchema.processingTimeInd).asInstanceOf[Double])
    root.put(rowSchema.contextField.name, element.getField(rowSchema.contextInd).asInstanceOf[String])
    out.write(mapper.writeValueAsBytes(root))
    out.close()
    out.toByteArray

  }
}
